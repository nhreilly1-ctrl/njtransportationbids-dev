"""
Run this script locally to generate your ADMIN_PASSWORD_HASH.
Then paste the output into Render as an environment variable.

Usage:
    python generate_password_hash.py
"""
import hashlib, getpass

pw = getpass.getpass("Enter your desired admin password: ")
h  = hashlib.sha256(pw.encode()).hexdigest()
print("\nSet this in Render environment variables:")
print(f"  ADMIN_PASSWORD_HASH = {h}")
print(f"  ADMIN_USERNAME      = admin   (or change to whatever you want)")
print(f"\nAlso set a random SECRET_KEY, e.g.:")
import secrets
print(f"  SECRET_KEY          = {secrets.token_hex(32)}")
