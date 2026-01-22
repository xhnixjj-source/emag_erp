"""Authentication service"""
import os
from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
import bcrypt
from sqlalchemy.orm import Session
from app.models.user import User, UserRole, UserStatus
from app.config import config

# JWT configuration
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify password"""
    try:
        # Handle both passlib format and raw bcrypt format
        if hashed_password.startswith('$2b$') or hashed_password.startswith('$2a$') or hashed_password.startswith('$2y$'):
            # Raw bcrypt hash
            password_bytes = plain_password.encode('utf-8')
            hash_bytes = hashed_password.encode('utf-8')
            return bcrypt.checkpw(password_bytes, hash_bytes)
        else:
            # Passlib format (shouldn't happen with new code, but for compatibility)
            password_bytes = plain_password.encode('utf-8')
            hash_bytes = hashed_password.encode('utf-8')
            return bcrypt.checkpw(password_bytes, hash_bytes)
    except Exception:
        return False


def get_password_hash(password: str) -> str:
    """Hash password using bcrypt directly"""
    password_bytes = password.encode('utf-8')
    # bcrypt has a 72-byte limit
    if len(password_bytes) > 72:
        password_bytes = password_bytes[:72]
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password_bytes, salt)
    return hashed.decode('utf-8')


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create JWT token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def authenticate_user(db: Session, username: str, password: str) -> Optional[User]:
    """Authenticate user"""
    user = db.query(User).filter(User.username == username).first()
    if not user:
        return None
    if user.status != UserStatus.ACTIVE:
        return None
    if not verify_password(password, user.password_hash):
        return None
    # Update last login time
    user.last_login_at = datetime.utcnow()
    db.commit()
    return user


def get_user_by_username(db: Session, username: str) -> Optional[User]:
    """Get user by username"""
    return db.query(User).filter(User.username == username).first()


def get_user_by_id(db: Session, user_id: int) -> Optional[User]:
    """Get user by ID"""
    return db.query(User).filter(User.id == user_id).first()


def create_user(db: Session, username: str, password: str, role: UserRole = UserRole.USER) -> User:
    """Create new user"""
    hashed_password = get_password_hash(password)
    user = User(
        username=username,
        password_hash=hashed_password,
        role=role,
        status=UserStatus.ACTIVE
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def update_user(
    db: Session,
    user_id: int,
    username: Optional[str] = None,
    password: Optional[str] = None,
    role: Optional[UserRole] = None,
    status: Optional[UserStatus] = None
) -> Optional[User]:
    """Update user"""
    user = get_user_by_id(db, user_id)
    if not user:
        return None
    
    if username is not None:
        # Check if username already exists (excluding current user)
        existing_user = db.query(User).filter(
            User.username == username,
            User.id != user_id
        ).first()
        if existing_user:
            return None
        user.username = username
    
    if password is not None:
        user.password_hash = get_password_hash(password)
    
    if role is not None:
        user.role = role
    
    if status is not None:
        user.status = status
    
    db.commit()
    db.refresh(user)
    return user


def delete_user(db: Session, user_id: int) -> bool:
    """Delete user"""
    user = get_user_by_id(db, user_id)
    if not user:
        return False
    
    db.delete(user)
    db.commit()
    return True
