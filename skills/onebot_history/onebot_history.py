#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""OneBot 群聊历史消息获取脚本

用法:
    # 方式 1: 从 session_id 获取（推荐，Agent 直接传 session_id）
    python3 onebot_history.py --session-id "onebot:group:3241818457:549149294"
    
    # 方式 2: 指定 group-id + instance
    python3 onebot_history.py --group-id 549149294 --instance napcat

鉴权机制:
    1. Token 从 agent.json 自动读取
    2. 通过 get_login_info API 获取实例的 bot QQ 号
    3. 匹配 session_id 中的 bot_qq 或直接使用指定实例
"""

import argparse
import asyncio
import aiohttp
import json
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict

# 配置文件路径
AGENT_CONFIG_PATH = Path("/root/.copaw/workspaces/default/agent.json")

# 缓存: instance_name -> bot_qq
_bot_qq_cache: Dict[str, int] = {}


def load_onebot_config() -> dict:
    """从 agent.json 加载 OneBot 配置"""
    with open(AGENT_CONFIG_PATH) as f:
        config = json.load(f)
    
    instances = config.get('channels', {}).get('onebot', {}).get('instances', [])
    return {inst['name']: inst for inst in instances}


def parse_session_id(session_id: str) -> Optional[dict]:
    """解析 OneBot session_id
    
    格式:
      - onebot:group:{bot_qq}:{group_id}
      - onebot:private:{bot_qq}:{user_id}
    """
    group_match = re.match(r'onebot:group:(\d+):(\d+)', session_id)
    private_match = re.match(r'onebot:private:(\d+):(\d+)', session_id)
    
    if group_match:
        return {
            "type": "group",
            "bot_qq": int(group_match.group(1)),
            "group_id": int(group_match.group(2)),
        }
    elif private_match:
        return {
            "type": "private",
            "bot_qq": int(private_match.group(1)),
            "user_id": int(private_match.group(2)),
        }
    return None


async def get_bot_qq(instance_name: str, token: str) -> Optional[int]:
    """通过 API 获取实例的 bot QQ 号"""
    # 检查缓存
    if instance_name in _bot_qq_cache:
        return _bot_qq_cache[instance_name]
    
    ws_url = f"ws://{instance_name}:3001"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as ws:
                await ws.send_json({
                    "action": "get_login_info",
                    "params": {},
                    "echo": "login_info"
                })
                
                for _ in range(5):
                    msg = await asyncio.wait_for(ws.receive(), timeout=3)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        result = json.loads(msg.data)
                        if result.get("echo") == "login_info":
                            if result.get("status") == "ok":
                                bot_qq = result.get("data", {}).get("user_id")
                                if bot_qq:
                                    _bot_qq_cache[instance_name] = bot_qq
                                    return bot_qq
                            return None
        return None
    except:
        return None


async def find_instance_by_qq(instances: dict, bot_qq: int) -> Optional[tuple]:
    """根据 bot QQ 号找到实例（遍历检查）"""
    for name, inst in instances.items():
        if not inst.get('enabled', True):
            continue
        
        token = inst.get('access_token', '')
        instance_qq = await get_bot_qq(name, token)
        
        if instance_qq == bot_qq:
            return name, inst
    
    return None, None


async def get_group_history(
    instance_name: str, 
    token: str, 
    group_id: int, 
    count: int = 20
) -> dict:
    """获取群历史消息"""
    ws_url = f"ws://{instance_name}:3001"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                ws_url, 
                headers=headers, 
                timeout=aiohttp.ClientTimeout(total=10)
            ) as ws:
                await ws.send_json({
                    "action": "get_group_msg_history",
                    "params": {
                        "group_id": group_id,
                        "count": count
                    },
                    "echo": "history_query"
                })
                
                for _ in range(10):
                    msg = await asyncio.wait_for(ws.receive(), timeout=5)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        result = json.loads(msg.data)
                        if result.get("echo") == "history_query":
                            if result.get("status") == "ok":
                                return format_messages(result.get("data", {}).get("messages", []))
                            else:
                                return {"error": result.get("message", "未知错误")}
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        return {"error": "WebSocket 连接错误"}
                        
                return {"error": "响应超时"}
                
    except asyncio.TimeoutError:
        return {"error": "连接超时"}
    except Exception as e:
        return {"error": str(e)}


def format_messages(messages: list) -> dict:
    """格式化消息列表"""
    formatted = []
    
    for msg in messages:
        content = ""
        raw_msg = msg.get("raw_message", "")
        msg_parts = msg.get("message", [])
        
        if msg_parts:
            texts = []
            for part in msg_parts:
                if part.get("type") == "text":
                    texts.append(part.get("data", {}).get("text", ""))
                elif part.get("type") == "at":
                    qq = part.get("data", {}).get("qq", "")
                    texts.append(f"@{qq}")
                elif part.get("type") == "image":
                    texts.append("[图片]")
            content = "".join(texts)
        else:
            content = raw_msg
        
        timestamp = msg.get("time", 0)
        time_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S") if timestamp else ""
        
        formatted.append({
            "time": time_str,
            "timestamp": timestamp,
            "user_id": msg.get("user_id"),
            "nickname": msg.get("sender", {}).get("nickname", ""),
            "card": msg.get("sender", {}).get("card", ""),
            "role": msg.get("sender", {}).get("role", ""),
            "message": content,
            "message_id": msg.get("message_id"),
            "group_id": msg.get("group_id"),
            "group_name": msg.get("group_name", ""),
        })
    
    return {
        "count": len(formatted),
        "messages": formatted
    }


async def main_async(args):
    """异步主函数"""
    instances = load_onebot_config()
    
    # 方式 1: 从 session_id 解析
    if args.session_id:
        parsed = parse_session_id(args.session_id)
        if not parsed:
            return {"error": f"无法解析 session_id: {args.session_id}"}
        
        if parsed.get("type") != "group":
            return {"error": "只支持获取群历史消息，私聊暂不支持"}
        
        group_id = parsed["group_id"]
        bot_qq = parsed["bot_qq"]
        
        # 根据 bot_qq 遍历查找实例
        instance_name, inst = await find_instance_by_qq(instances, bot_qq)
        
        if not instance_name:
            return {"error": f"未找到 QQ 号为 {bot_qq} 的 bot 实例"}
        
        if not args.raw and not args.quiet:
            print(f"session_id: {args.session_id}")
            print(f"instance: {instance_name}, bot_qq: {bot_qq}, group_id: {group_id}")
        
        return await get_group_history(
            instance_name,
            inst.get('access_token', ''),
            group_id,
            args.count
        )
    
    # 方式 2: 指定 instance + group_id
    if args.instance:
        if args.instance not in instances:
            return {"error": f"实例 '{args.instance}' 不存在，可用: {list(instances.keys())}"}
        
        inst = instances[args.instance]
        return await get_group_history(
            args.instance,
            inst.get('access_token', ''),
            args.group_id,
            args.count
        )
    
    return {"error": "请指定 --session-id 或 --group-id + --instance"}


def main():
    parser = argparse.ArgumentParser(description="获取 OneBot 群聊历史消息")
    
    parser.add_argument("--session-id", default="", help="OneBot session_id（自动解析 bot_qq 和 group_id）")
    parser.add_argument("--group-id", type=int, default=0, help="群号")
    parser.add_argument("--instance", default="", help="实例名（napcat / napcat2）")
    parser.add_argument("--count", type=int, default=20, help="消息数量（默认 20）")
    parser.add_argument("--raw", action="store_true", help="输出原始 JSON 格式")
    parser.add_argument("--quiet", action="store_true", help="静默模式，不输出额外信息")
    
    args = parser.parse_args()
    
    result = asyncio.run(main_async(args))
    
    if args.raw:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        if "error" in result:
            print(f"错误: {result['error']}")
            sys.exit(1)
        
        messages = result.get("messages", [])
        print(f"\n=== 群聊历史消息（共 {len(messages)} 条）===\n")
        
        for msg in reversed(messages):
            nickname = msg.get("card") or msg.get("nickname") or str(msg.get("user_id"))
            time_str = msg.get("time", "")
            content = msg.get("message", "")
            print(f"[{time_str}] {nickname}: {content}")
        
        print()


if __name__ == "__main__":
    main()
