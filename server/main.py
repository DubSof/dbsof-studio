from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from enum import Enum

import schemas

app = FastAPI(title="Bookstore API", version="1.0.0")


# --- Models ---

class Genre(str, Enum):
    fiction = "fiction"
    nonfiction = "nonfiction"
    science = "science"
    history = "history"


class BookCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    author: str
    genre: Genre
    price: float = Field(..., gt=0)


class Book(BookCreate):
    id: int


# --- In-memory store ---

books_db: dict[int, Book] = {}
next_id = 1


# --- Routes ---

@app.get("/")
def root():
    return {"message": "Welcome to the Bookstore API"}


@app.post("/books", response_model=Book, status_code=201)
def create_book(book: BookCreate):
    """Create a new book entry."""
    global next_id
    new_book = Book(id=next_id, **book.model_dump())
    books_db[next_id] = new_book
    next_id += 1
    return new_book


@app.get("/books", response_model=list[Book])
def list_books(
    genre: Genre | None = None,
    max_price: float | None = Query(None, gt=0, description="Filter by max price"),
):
    """List all books with optional genre and price filters."""
    results = list(books_db.values())
    if genre:
        results = [b for b in results if b.genre == genre]
    if max_price is not None:
        results = [b for b in results if b.price <= max_price]
    return results


@app.get("/books/{book_id}", response_model=Book)
def get_book(book_id: int):
    """Retrieve a single book by ID."""
    if book_id not in books_db:
        raise HTTPException(status_code=404, detail="Book not found")
    return books_db[book_id]


@app.put("/books/{book_id}", response_model=Book)
def update_book(book_id: int, book: BookCreate):
    """Replace an existing book's data."""
    if book_id not in books_db:
        raise HTTPException(status_code=404, detail="Book not found")
    updated = Book(id=book_id, **book.model_dump())
    books_db[book_id] = updated
    return updated


@app.delete("/books/{book_id}", status_code=204)
def delete_book(book_id: int):
    """Delete a book by ID."""
    if book_id not in books_db:
        raise HTTPException(status_code=404, detail="Book not found")
    del books_db[book_id]
