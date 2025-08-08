#!/usr/bin/env python3
"""
Debug arrow keys to see what sequences we get
"""

import sys
import termios
import tty

def debug_keys():
    print("Key Debug Mode - Press keys to see their codes")
    print("Press 'q' to quit, or arrow keys to see their sequences")
    print("=" * 50)
    
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    
    try:
        tty.setraw(sys.stdin.fileno())
        
        while True:
            key = sys.stdin.read(1)
            
            if key == 'q':
                break
            elif key == '\x1b':  # ESC
                print(f"ESC sequence: \\x1b (27)")
                # Try to read more characters
                try:
                    # Set non-blocking
                    import select
                    chars = [key]
                    
                    # Read up to 3 more characters with timeout
                    for i in range(3):
                        if select.select([sys.stdin], [], [], 0.1)[0]:
                            next_char = sys.stdin.read(1)
                            chars.append(next_char)
                        else:
                            break
                    
                    full_sequence = ''.join(chars)
                    print(f"Full sequence: {repr(full_sequence)}")
                    print(f"Bytes: {[ord(c) for c in chars]}")
                    
                    if len(chars) >= 3 and chars[1] == '[':
                        if chars[2] == 'A':
                            print("-> UP ARROW")
                        elif chars[2] == 'B':
                            print("-> DOWN ARROW")
                        elif chars[2] == 'C':
                            print("-> RIGHT ARROW")
                        elif chars[2] == 'D':
                            print("-> LEFT ARROW")
                        else:
                            print(f"-> Unknown arrow: {chars[2]}")
                    else:
                        print("-> Just ESC")
                        
                except Exception as e:
                    print(f"Error reading sequence: {e}")
            else:
                print(f"Key: {repr(key)} (ord: {ord(key)})")
            
            print("-" * 30)
                
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        print("\nExiting debug mode")

if __name__ == "__main__":
    debug_keys()
