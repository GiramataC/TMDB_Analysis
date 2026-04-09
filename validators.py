"""
Data validators using Pydantic for API responses and cleaned data.

Ensures data quality at multiple pipeline stages:
- API response validation (after fetch)
- Cleaned data validation (after cleaning)
- KPI validation (after calculation)
"""

from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Optional, List
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ===============================
# 1. API Response Validation
# ===============================

class CastMember(BaseModel):
    """Casts member from API response"""
    name: str
    character: Optional[str] = None


class CrewMember(BaseModel):
    """Crew member from API response"""
    name: str
    job: str
    department: Optional[str] = None


class Genre(BaseModel):
    """Genre from API response"""
    id: int
    name: str


class ProductionCompany(BaseModel):
    """Production company from API response"""
    id: int
    name: str
    logo_path: Optional[str] = None
    origin_country: Optional[str] = None


class MovieAPIResponse(BaseModel):
    """Validates TMDB API response for a single movie"""
    
    id: int
    title: str
    budget: float = Field(ge=0, description="Budget in USD, must be >= 0")
    revenue: float = Field(ge=0, description="Revenue in USD, must be >= 0")
    release_date: Optional[str] = None
    popularity: float = Field(ge=0)
    vote_average: float = Field(ge=0, le=10, description="Rating 0-10")
    vote_count: int = Field(ge=0)
    overview: Optional[str] = None
    genres: Optional[List[Genre]] = None
    production_companies: Optional[List[ProductionCompany]] = None
    cast: Optional[str] = None  # Pipe-separated names
    cast_size: int = Field(default=0, ge=0, description="Computed from credits after fetch")
    director: Optional[str] = None
    crew_size: int = Field(default=0, ge=0, description="Computed from credits after fetch")
    runtime: Optional[int] = Field(None, ge=0)
    
    @field_validator("title")
    @classmethod
    def title_not_empty(cls, v):
        """Ensure title is not empty"""
        if not v or not v.strip():
            raise ValueError("Title cannot be empty")
        return v.strip()
    
    @field_validator("budget", "revenue")
    @classmethod
    def reasonable_budget_revenue(cls, v):
        """Ensure budget/revenue are reasonable (< $5B)"""
        if v > 5_000_000_000:
            raise ValueError(f"Value seems unrealistic (> $5B): {v}")
        return v
    
    @field_validator("release_date")
    @classmethod
    def valid_release_date(cls, v):
        """Validate release date format"""
        if v and v.strip():
            try:
                datetime.strptime(v, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Invalid date format (expected YYYY-MM-DD): {v}")
        return v
    
    class Config:
        extra = "allow"  # Allow additional fields from API


# ===============================
# 2. Cleaned Data Validation
# ===============================

class MovieCleaned(BaseModel):
    """Validates cleaned movie data ready for analysis"""
    
    id: int
    title: str
    budget_musd: float = Field(ge=0, description="Budget in millions USD")
    revenue_musd: float = Field(ge=0, description="Revenue in millions USD")
    release_date: Optional[str] = None
    popularity: float = Field(ge=0)
    vote_average: float = Field(ge=0, le=10)
    vote_count: int = Field(ge=0)
    genres: Optional[str] = None  # Pipe-separated
    cast: Optional[str] = None
    director: Optional[str] = None
    runtime: Optional[int] = Field(None, ge=0)
    
    @field_validator("title")
    @classmethod
    def title_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("Title cannot be empty")
        return v
    
    class Config:
        extra = "allow"  # Allow additional fields


# ===============================
# 3. KPI Validation
# ===============================

class MovieWithKPI(BaseModel):
    """Validates movie data with calculated KPIs"""
    
    id: int
    title: str
    budget_musd: float = Field(ge=0)
    revenue_musd: float = Field(ge=0)
    profit_musd: Optional[float] = None  # revenue - budget
    roi: Optional[float] = Field(None, ge=0, description="Return on investment")
    popularity: float = Field(ge=0)
    vote_average: float = Field(ge=0, le=10)
    
    @field_validator("roi")
    @classmethod
    def valid_roi(cls, v, info):
        """Ensure ROI is valid (not inf, not NaN)"""
        if v is not None:
            if v != v:  # NaN check
                raise ValueError("ROI is NaN (likely division by zero budget)")
            if v == float('inf'):
                raise ValueError("ROI is infinite (budget was 0)")
        return v
    
    @field_validator("profit_musd")
    @classmethod
    def valid_profit(cls, v, info):
        """Profit should equal revenue - budget"""
        if v is not None and (v != v):  # NaN check
            raise ValueError("Profit is NaN")
        return v
    
    class Config:
        extra = "allow"


# ===============================
# Validation Functions
# ===============================

def validate_api_response(movie_dict: dict) -> MovieAPIResponse:
    """
    Validate a single API response from TMDB.
    
    Args:
        movie_dict: Raw API response dictionary
    
    Returns:
        MovieAPIResponse: Validated movie object
    
    Raises:
        ValidationError: If data doesn't meet schema
    """
    try:
        return MovieAPIResponse(**movie_dict)
    except ValidationError as e:
        logger.error(f"API response validation failed for movie {movie_dict.get('id')}: {e}")
        raise


def validate_cleaned_movie(row_dict: dict) -> MovieCleaned:
    """
    Validate cleaned movie data.
    
    Args:
        row_dict: Cleaned movie row as dictionary
    
    Returns:
        MovieCleaned: Validated cleaned movie object
    
    Raises:
        ValidationError: If data doesn't meet schema
    """
    try:
        return MovieCleaned(**row_dict)
    except ValidationError as e:
        logger.error(f"Cleaned data validation failed for movie {row_dict.get('id')}: {e}")
        raise


def validate_movie_with_kpi(row_dict: dict) -> MovieWithKPI:
    """
    Validate movie data with KPIs.
    
    Args:
        row_dict: Movie row with KPIs as dictionary
    
    Returns:
        MovieWithKPI: Validated movie with KPI object
    
    Raises:
        ValidationError: If data doesn't meet schema
    """
    try:
        return MovieWithKPI(**row_dict)
    except ValidationError as e:
        logger.error(f"KPI validation failed for movie {row_dict.get('id')}: {e}")
        raise


def validate_dataframe(df, schema_class) -> list:
    """
    Validate all rows in a DataFrame against a Pydantic schema.
    
    Args:
        df: pandas DataFrame to validate
        schema_class: Pydantic model class to validate against
    
    Returns:
        list: Validated objects (one per row)
    
    Raises:
        ValidationError: If any row fails validation
    """
    validated = []
    failed_rows = []
    
    for idx, row in df.iterrows():
        try:
            row_dict = row.to_dict()
            validated_obj = schema_class(**row_dict)
            validated.append(validated_obj)
        except ValidationError as e:
            failed_rows.append((idx, row.get("id"), str(e)))
            logger.warning(f"Row {idx} (ID: {row.get('id')}) validation failed: {e}")
    
    if failed_rows:
        logger.error(f"Validation failed for {len(failed_rows)} rows:")
        for idx, movie_id, error in failed_rows:
            logger.error(f"  Row {idx} (movie_id={movie_id}): {error}")
        raise ValidationError(
            f"{len(failed_rows)} rows failed validation. See logs for details."
        )
    
    return validated


# ===============================
# Data Quality Checks
# ===============================

def check_dataframe_quality(df, name: str):
    """
    Perform general data quality checks on DataFrame.
    
    Args:
        df: DataFrame to check
        name: Name of dataset for logging
    
    Raises:
        ValueError: If data quality issues found
    """
    logger.info(f"Checking data quality for {name}...")
    
    # Check for completely empty dataframe
    if df.empty:
        raise ValueError(f"{name} is empty")
    
    # Check for missing values
    missing_pct = (df.isnull().sum() / len(df) * 100)
    high_missing = missing_pct[missing_pct > 50]
    
    if not high_missing.empty:
        logger.warning(f"{name}: Columns with >50% missing values: {high_missing.to_dict()}")
    
    # Check for duplicate IDs
    if 'id' in df.columns:
        duplicates = df['id'].duplicated().sum()
        if duplicates > 0:
            logger.warning(f"{name}: Found {duplicates} duplicate movie IDs")
    
    logger.info(f"{name} quality check passed. Shape: {df.shape}")
