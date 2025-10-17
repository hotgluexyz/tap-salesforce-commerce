import re

def cover_access_token(response_text: str) -> str:
    """Cover access token in response text to avoid exposing sensitive data in logs.
    
    Args:
        response_text: The response text that may contain an access token
        
    Returns:
        The response text with access token covered if present
    """
    if '"accessToken"' in response_text:
        # Match the access token value after "accessToken":
        covered = re.sub(r'"accessToken"\s*:\s*"[^"]*"', '"accessToken": "****"', response_text)
        return covered
    return response_text
