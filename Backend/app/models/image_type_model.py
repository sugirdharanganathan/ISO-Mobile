# app/models/image_type_model.py
from sqlalchemy import Column, Integer, String, Text, DateTime, func
from app.database import Base


class ImageType(Base):
    """
    image_type table:
    - id: auto-increment PK
    - image_type: name of the image type
    - description: optional description text
    """

    __tablename__ = "image_type"

    id = Column(Integer, primary_key=True, autoincrement=True)
    image_type = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)

    created_at = Column(DateTime(), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(), server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<ImageType id={self.id} image_type='{self.image_type}'>"
