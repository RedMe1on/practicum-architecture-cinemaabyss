import datetime
from pydantic import BaseModel


class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: int | None = None
    rating: float | None = None
    genres: list[str] | None = None
    description: str | None = None


class UserEvent(BaseModel):
    user_id: int
    username: str | None = None
    email: str | None = None
    action: str
    timestamp: datetime.datetime


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: datetime.datetime
    method_type: str | None = None
