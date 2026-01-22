"""Authentication API"""
from datetime import timedelta, datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from pydantic import BaseModel
from app.database import get_db
from app.services.auth_service import (
    authenticate_user,
    create_access_token,
    get_user_by_id,
    create_user as create_user_service,
    update_user as update_user_service,
    delete_user as delete_user_service,
    SECRET_KEY,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
from app.models.user import User, UserRole, UserStatus
from app.middleware.auth_middleware import get_current_user, require_auth
from app.services.operation_log_service import create_operation_log
from jose import JWTError, jwt

router = APIRouter(prefix="/api/auth", tags=["auth"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


class LoginRequest(BaseModel):
    """Login request model"""
    username: str
    password: str


class LoginResponse(BaseModel):
    """Login response model"""
    access_token: str
    token_type: str = "bearer"
    user: dict


class UserResponse(BaseModel):
    """User response model"""
    id: int
    username: str
    role: str
    status: str
    created_at: Optional[str] = None
    last_login_at: Optional[str] = None

    class Config:
        from_attributes = True


class CreateUserRequest(BaseModel):
    """Create user request model"""
    username: str
    password: str
    role: Optional[str] = "user"


class UpdateUserRequest(BaseModel):
    """Update user request model"""
    username: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None
    status: Optional[str] = None


class UpdateUserStatusRequest(BaseModel):
    """Update user status request model"""
    status: str


@router.post("/login", response_model=LoginResponse)
async def login(
    login_data: LoginRequest,
    request: Request,
    db: Session = Depends(get_db)
):
    """User login"""
    user = authenticate_user(db, login_data.username, login_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": str(user.id), "username": user.username, "role": user.role.value},
        expires_delta=access_token_expires
    )
    
    # Log login operation
    ip_address = request.client.host if request.client else None
    create_operation_log(
        db=db,
        user_id=user.id,
        operation_type="login",
        target_type="user",
        target_id=user.id,
        operation_detail={"username": user.username},
        ip_address=ip_address
    )
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "username": user.username,
            "role": user.role.value,
            "status": user.status.value
        }
    }


@router.post("/logout")
async def logout(
    request: Request,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """User logout"""
    # Log logout operation
    ip_address = request.client.host if request.client else None
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="logout",
        target_type="user",
        target_id=current_user["id"],
        operation_detail={"username": current_user["username"]},
        ip_address=ip_address
    )
    # In JWT-based auth, logout is typically handled client-side by removing the token
    return {"message": "Logged out successfully"}


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get current user information"""
    user = get_user_by_id(db, current_user["id"])
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # 将datetime对象转换为ISO格式字符串
    return UserResponse(
        id=user.id,
        username=user.username,
        role=user.role.value,
        status=user.status.value,
        created_at=user.created_at.isoformat() if user.created_at else None,
        last_login_at=user.last_login_at.isoformat() if user.last_login_at else None
    )


@router.post("/users", response_model=UserResponse)
async def create_user(
    user_data: CreateUserRequest,
    request: Request,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Create new user (admin only)"""
    # Check if current user is admin
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can create users"
        )
    
    # Check if username already exists
    from app.models.user import User
    existing_user = db.query(User).filter(User.username == user_data.username).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already exists"
        )
    
    role = UserRole.ADMIN if user_data.role == "admin" else UserRole.USER
    user = create_user_service(db, user_data.username, user_data.password, role)
    
    # Log user creation operation
    ip_address = request.client.host if request.client else None
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="user_create",
        target_type="user",
        target_id=user.id,
        operation_detail={
            "created_username": user.username,
            "created_role": user.role.value
        },
        ip_address=ip_address
    )
    
    # 将datetime对象转换为ISO格式字符串
    return UserResponse(
        id=user.id,
        username=user.username,
        role=user.role.value,
        status=user.status.value,
        created_at=user.created_at.isoformat() if user.created_at else None,
        last_login_at=user.last_login_at.isoformat() if user.last_login_at else None
    )


@router.get("/users", response_model=list[UserResponse])
async def list_users(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """List all users (admin only)"""
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can list users"
        )
    
    users = db.query(User).offset(skip).limit(limit).all()
    # 将datetime对象转换为ISO格式字符串
    return [
        UserResponse(
            id=user.id,
            username=user.username,
            role=user.role.value,
            status=user.status.value,
            created_at=user.created_at.isoformat() if user.created_at else None,
            last_login_at=user.last_login_at.isoformat() if user.last_login_at else None
        )
        for user in users
    ]


@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get user by ID (admin only)"""
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can view user details"
        )
    
    user = get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # 将datetime对象转换为ISO格式字符串
    return UserResponse(
        id=user.id,
        username=user.username,
        role=user.role.value,
        status=user.status.value,
        created_at=user.created_at.isoformat() if user.created_at else None,
        last_login_at=user.last_login_at.isoformat() if user.last_login_at else None
    )


@router.put("/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: int,
    user_data: UpdateUserRequest,
    request: Request,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Update user (admin only)"""
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can update users"
        )
    
    # Get the user to update
    user = get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Prepare update parameters
    role = None
    if user_data.role is not None:
        role = UserRole.ADMIN if user_data.role == "admin" else UserRole.USER
    
    status_value = None
    if user_data.status is not None:
        status_value = UserStatus.ACTIVE if user_data.status == "active" else UserStatus.INACTIVE
    
    # Update user
    updated_user = update_user_service(
        db=db,
        user_id=user_id,
        username=user_data.username,
        password=user_data.password,
        role=role,
        status=status_value
    )
    
    if not updated_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to update user. Username may already exist."
        )
    
    # Log user update operation
    ip_address = request.client.host if request.client else None
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="user_update",
        target_type="user",
        target_id=user_id,
        operation_detail={
            "updated_username": updated_user.username,
            "updated_role": updated_user.role.value,
            "updated_status": updated_user.status.value
        },
        ip_address=ip_address
    )
    
    # 将datetime对象转换为ISO格式字符串
    return UserResponse(
        id=updated_user.id,
        username=updated_user.username,
        role=updated_user.role.value,
        status=updated_user.status.value,
        created_at=updated_user.created_at.isoformat() if updated_user.created_at else None,
        last_login_at=updated_user.last_login_at.isoformat() if updated_user.last_login_at else None
    )


@router.put("/users/{user_id}/status", response_model=UserResponse)
async def update_user_status(
    user_id: int,
    status_data: UpdateUserStatusRequest,
    request: Request,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Update user status (admin only)"""
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can update user status"
        )
    
    user = get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    status_value = UserStatus.ACTIVE if status_data.status == "active" else UserStatus.INACTIVE
    updated_user = update_user_service(db=db, user_id=user_id, status=status_value)
    
    if not updated_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Failed to update user status"
        )
    
    # Log status change operation
    ip_address = request.client.host if request.client else None
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="status_change",
        target_type="user",
        target_id=user_id,
        operation_detail={
            "old_status": user.status.value,
            "new_status": updated_user.status.value,
            "username": updated_user.username
        },
        ip_address=ip_address
    )
    
    # 将datetime对象转换为ISO格式字符串
    return UserResponse(
        id=updated_user.id,
        username=updated_user.username,
        role=updated_user.role.value,
        status=updated_user.status.value,
        created_at=updated_user.created_at.isoformat() if updated_user.created_at else None,
        last_login_at=updated_user.last_login_at.isoformat() if updated_user.last_login_at else None
    )


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: int,
    request: Request,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Delete user (admin only)"""
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can delete users"
        )
    
    # Prevent self-deletion
    if user_id == current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )
    
    user = get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Log user deletion operation before deleting
    ip_address = request.client.host if request.client else None
    create_operation_log(
        db=db,
        user_id=current_user["id"],
        operation_type="user_delete",
        target_type="user",
        target_id=user_id,
        operation_detail={
            "deleted_username": user.username,
            "deleted_role": user.role.value
        },
        ip_address=ip_address
    )
    
    # Delete user
    success = delete_user_service(db, user_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete user"
        )
    
    return {"message": "User deleted successfully"}

