from ua_parser import user_agent_parser


def get_browser_family(user_agent_string):
    parsed = user_agent_parser.ParseUserAgent(user_agent_string)
    return parsed["family"]
