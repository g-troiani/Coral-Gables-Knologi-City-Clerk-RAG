#!/usr/bin/env python3
"""
Debug the enhanced query system to see why the answer might be empty.
"""

import subprocess
import sys
from pathlib import Path

def test_enhanced_command():
    """Test the exact command our enhanced system generates."""
    venv_python = Path('venv/bin/python3')
    
    # The exact command our enhanced system generates
    cmd = [
        str(venv_python),
        '-m', 'graphrag', 'query',
        '--root', 'graphrag_data',
        '--method', 'local',
        '--entity-filter', 'type=AGENDA_ITEM;title=E-1',
        '--top-k', '1',
        '--no-community-context',
        '--query', 'What is agenda item E-1?'
    ]
    
    print('Enhanced system command:')
    print(' '.join(cmd))
    print()
    
    print('Executing enhanced command...')
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print('\nRESULT:')
    print('STDOUT:', result.stdout)
    print('STDERR:', result.stderr)
    print('Return code:', result.returncode)
    
    # Also try without entity filter to see if that's the issue
    print('\n' + '='*60)
    print('Testing without entity filter for comparison:')
    
    cmd_no_filter = [
        str(venv_python),
        '-m', 'graphrag', 'query',
        '--root', 'graphrag_data',
        '--method', 'local',
        '--top-k', '1',
        '--no-community-context',
        '--query', 'What is agenda item E-1?'
    ]
    
    print(' '.join(cmd_no_filter))
    result2 = subprocess.run(cmd_no_filter, capture_output=True, text=True)
    print('\nRESULT WITHOUT FILTER:')
    print('STDOUT:', result2.stdout[:500] + '...' if len(result2.stdout) > 500 else result2.stdout)
    print('STDERR:', result2.stderr)
    print('Return code:', result2.returncode)

if __name__ == "__main__":
    test_enhanced_command() 