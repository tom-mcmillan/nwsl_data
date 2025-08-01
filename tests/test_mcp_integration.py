#!/usr/bin/env python3
"""
Test MCP Integration - Verify tools work with Inspector format
"""

import json
from mcp_server_config import handle_tool_call, create_mcp_server

def test_tool_integration():
    """
    Test each tool to ensure MCP compatibility
    """
    print("🧪 Testing MCP Tool Integration")
    print("=" * 50)
    
    # Test 1: Server configuration
    print("\n1. Testing server configuration...")
    config = create_mcp_server()
    print(f"✅ Server configured with {len(config['tools'])} tools")
    
    # Test 2: Database overview (no parameters)
    print("\n2. Testing database overview...")
    result = handle_tool_call("get_database_overview", {})
    if "error" not in result:
        data = json.loads(result["content"])
        print(f"✅ Database overview: {data['total_seasons']} seasons, {data['total_matches']} matches")
    else:
        print(f"❌ Database overview failed: {result['error']}")
    
    # Test 3: Team search (with parameter)
    print("\n3. Testing team search...")
    result = handle_tool_call("search_teams", {"search_term": "Courage"})
    if "error" not in result:
        data = json.loads(result["content"])
        print(f"✅ Team search: Found {data['matches_found']} teams")
        if data['matches_found'] > 0:
            print(f"   - {data['teams'][0]['primary_name']}")
    else:
        print(f"❌ Team search failed: {result['error']}")
    
    # Test 4: Season summary (with parameter)
    print("\n4. Testing season summary...")
    result = handle_tool_call("get_season_summary", {"season_id": 2025})
    if "error" not in result:
        data = json.loads(result["content"])
        print(f"✅ Season summary: {data['total_matches']} matches, {data['teams_count']} teams")
    else:
        print(f"❌ Season summary failed: {result['error']}")
    
    # Test 5: Team analysis (multiple parameters)
    print("\n5. Testing team analysis...")
    result = handle_tool_call("analyze_team_season", {
        "team_name": "North Carolina Courage", 
        "season_id": 2025
    })
    if "error" not in result:
        data = json.loads(result["content"])
        if "error" not in data:
            stats = data['overall_stats']
            print(f"✅ Team analysis: {stats['matches_played']} matches, {stats['total_goals_scored']} goals")
        else:
            print(f"❌ Team analysis returned error: {data['error']}")
    else:
        print(f"❌ Team analysis failed: {result['error']}")
    
    # Test 6: Parameter validation
    print("\n6. Testing parameter validation...")
    result = handle_tool_call("validate_query_parameters", {
        "team_name": "North Carolina Courage", 
        "season_id": 2025
    })
    if "error" not in result:
        data = json.loads(result["content"])
        print(f"✅ Validation: {'Valid' if data.get('valid') else 'Invalid'} query")
    else:
        print(f"❌ Validation failed: {result['error']}")
    
    print("\n" + "=" * 50)
    print("🎯 MCP Integration Test Complete")

def generate_inspector_examples():
    """
    Generate example calls for the MCP Inspector
    """
    examples = {
        "database_overview": {
            "tool": "get_database_overview",
            "arguments": {},
            "description": "Get overview of database contents and seasons"
        },
        "search_courage": {
            "tool": "search_teams", 
            "arguments": {"search_term": "Courage"},
            "description": "Search for teams matching 'Courage'"
        },
        "2025_season": {
            "tool": "get_season_summary",
            "arguments": {"season_id": 2025},
            "description": "Get 2025 season summary"
        },
        "courage_2025": {
            "tool": "analyze_team_season",
            "arguments": {"team_name": "North Carolina Courage", "season_id": 2025},
            "description": "Analyze North Carolina Courage's 2025 season"
        },
        "validate_courage": {
            "tool": "validate_query_parameters",
            "arguments": {"team_name": "Courage", "season_id": 2025},
            "description": "Validate if 'Courage' team exists in 2025"
        }
    }
    
    print("\n📋 MCP Inspector Example Calls:")
    print("=" * 50)
    for name, example in examples.items():
        print(f"\n{name.upper()}:")
        print(f"Tool: {example['tool']}")
        print(f"Args: {json.dumps(example['arguments'])}")
        print(f"Desc: {example['description']}")

if __name__ == "__main__":
    test_tool_integration()
    generate_inspector_examples()