from functools import partial
from typing import Dict, Any

import hashlib

from sqlalchemy import Boolean, Column, Date, DateTime, Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship

from src.config.enums import AddressType, DigitalFootprintType, PersonalIdentityType, SourceCategory


Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    # Columns
    id = Column(Integer, primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(320), unique=True, nullable=False)
    password = Column(String(255), nullable=False)
    birth_date = Column(Date, nullable=False)
    phone = Column(String(25), nullable=False)

    # Relationships
    user_relationship = partial(
        relationship,
        back_populates="user",
        cascade="all, delete-orphan"
    )
    secondary_emails = user_relationship("SecondaryEmail")
    secondary_phones = user_relationship("SecondaryPhone")
    addresses = user_relationship("Address")
    pictures = user_relationship("Picture")
    digital_footprints = user_relationship("UserDigitalFootprint")

    def __repr__(self):
        return f"<User(id={self.id}, name='{self.first_name} {self.last_name}')>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'email': self.email,
            'password': self.password,
            'phone': self.phone,
            'birth_date': self.birth_date.isoformat() if self.birth_date else None,
            'birth_year': self.birth_date.year if self.birth_date else None,
            'addresses': [
                {
                    'type': addr.type,
                    'country': addr.country,
                    'city': addr.city,
                    'street': addr.street,
                    'number': addr.number
                }
                for addr in self.addresses
            ],
            'secondary_emails': [email.email for email in self.secondary_emails],
            'secondary_phones': [phone.phone for phone in self.secondary_phones],
            'pictures': [picture.path for picture in self.pictures],
            'digital_footprints': [
                {
                    'id': df.digital_footprint.id,
                    'type': df.digital_footprint.type,
                    'media_filepath': df.digital_footprint.media_filepath,
                    'reference_url': df.digital_footprint.reference_url,
                    'source_id': df.digital_footprint.source_id
                }
                for df in self.digital_footprints
            ]
        }


class SecondaryEmail(Base):
    __tablename__ = 'secondary_emails'

    # Columns
    user_id = Column(Integer, ForeignKey('users.id'), primary_key=True)
    email = Column(String(320), primary_key=True)

    # Relationships
    user = relationship("User", back_populates="secondary_emails")

    def __repr__(self):
        return f"<SecondaryEmail(user_id={self.user_id}, email='{self.email}')>"


class SecondaryPhone(Base):
    __tablename__ = 'secondary_phones'

    # Columns
    user_id = Column(Integer, ForeignKey('users.id'), primary_key=True)
    phone = Column(String(25), primary_key=True)

    # Relationships
    user = relationship("User", back_populates="secondary_phones")

    def __repr__(self):
        return f"<SecondaryPhone(user_id={self.user_id}, phone='{self.phone}')>"


class Address(Base):
    __tablename__ = 'addresses'

    # Columns
    user_id = Column(Integer, ForeignKey('users.id'), primary_key=True)
    type = Column(Enum(AddressType), primary_key=True)
    country = Column(String(100), primary_key=True)
    city = Column(String(100), primary_key=True)
    street = Column(String(200), primary_key=True)
    number = Column(Integer, primary_key=True)

    # Relationships
    user = relationship("User", back_populates="addresses")

    def __repr__(self):
        return (f"<Address(user_id={self.user_id}, type='{self.type}', "
                f"address='{self.number} {self.street}, {self.city}, {self.country}')>")


class Picture(Base):
    __tablename__ = 'pictures'

    # Columns
    user_id = Column(Integer, ForeignKey('users.id'), primary_key=True)
    path = Column(String(500), primary_key=True)

    # Relationships
    user = relationship("User", back_populates="pictures")

    def __repr__(self):
        return f"<Picture(user_id={self.user_id}, path='{self.path}')>"


class DigitalFootprint(Base):
    __tablename__ = 'digital_footprints'

    # Columns
    id = Column(Integer, primary_key=True)
    type = Column(Enum(DigitalFootprintType), nullable=False)
    media_filepath = Column(String(255), nullable=True)
    reference_url = Column(String(255), nullable=False)
    source_id = Column(Integer, ForeignKey('sources.id'), nullable=False)

    __table_args__ = (
        UniqueConstraint('media_filepath', 'reference_url', name='uq_media_filepath_reference_url'),
    )

    # Relationships
    personal_identities = relationship("PersonalIdentity", back_populates="digital_footprint")
    source = relationship("Source", back_populates="digital_footprints")
    users = relationship("UserDigitalFootprint", back_populates="digital_footprint", cascade="all, delete-orphan")
    activity_logs = relationship("ActivityLog", back_populates="digital_footprint", cascade="all, delete-orphan")

    def __init__(self, type, reference_url, source_id, media_filepath=None,
                 generate_id=False, **kwargs):
        """Initialize DigitalFootprint instance."""
        super().__init__(**kwargs)

        self.type = type
        self.reference_url = reference_url
        self.source_id = source_id
        self.media_filepath = media_filepath

        if generate_id:
            self.id = self._generate_hash_id()

    def _generate_hash_id(self) -> int:
        """Generate a hash-based ID from reference_url and media_filepath."""
        combined_string = f"{self.reference_url}{self.media_filepath or ''}"

        # Create SHA-256 hash
        hash_object = hashlib.sha256(combined_string.encode('utf-8'))
        hash_hex = hash_object.hexdigest()

        # Convert first 8 characters of hex to integer (reasonable size)
        hash_int = int(hash_hex[:7], 16)

        return hash_int

    def __repr__(self):
        return f"<DigitalFootprint(id={self.id}, type='{self.type}', media_filepath='{self.media_filepath}', reference_url='{self.reference_url}', source_id={self.source_id})>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'type': self.type if self.type else None,
            'media_filepath': self.media_filepath,
            'reference_url': self.reference_url,
            'source_id': self.source_id,
            'personal_identities': [
                {
                    'digital_footprint_id': pi.digital_footprint_id,
                    'personal_identity': pi.personal_identity if pi.personal_identity else None
                }
                for pi in self.personal_identities
            ],
            'source': {
                'id': self.source.id,
                'name': self.source.name,
                'url': self.source.url,
                'category': self.source.category if self.source.category else None,
                'verified': self.source.verified
            } if self.source else None,
            'users': [
                {
                    'digital_footprint_id': udf.digital_footprint_id,
                    'user_id': udf.user_id
                }
                for udf in self.users
            ],
            'activity_logs': [
                {
                    'digital_footprint_id': al.digital_footprint_id,
                    'timestamp': al.timestamp.isoformat() if al.timestamp else None
                }
                for al in self.activity_logs
            ]
        }


class PersonalIdentity(Base):
    __tablename__ = 'personal_identities'

    # Columns
    digital_footprint_id = Column(Integer, ForeignKey('digital_footprints.id'), primary_key=True)
    personal_identity = Column(Enum(PersonalIdentityType), primary_key=True)

    # Relationship
    digital_footprint = relationship("DigitalFootprint", back_populates='personal_identities')

    def __repr__(self):
        return (f"<PersonalIdentity(digital_footprint_id={self.digital_footprint_id}, "
                f"personal_identity='{self.personal_identity}')>")

    def to_dict(self) -> Dict[str, Any]:
        return {
            'digital_footprint_id': self.digital_footprint_id,
            'personal_identity': self.personal_identity if self.personal_identity else None,
            'digital_footprint': {
                'id': self.digital_footprint.id,
                'type': self.digital_footprint.type if self.digital_footprint.type else None,
                'media_filepath': self.digital_footprint.media_filepath,
                'reference_url': self.digital_footprint.reference_url,
                'source_id': self.digital_footprint.source_id
            } if self.digital_footprint else None
        }


class UserDigitalFootprint(Base):
    __tablename__ = 'users_digital_footprints'

    # Columns
    digital_footprint_id = Column(Integer, ForeignKey('digital_footprints.id'), primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), primary_key=True)

    # Relationship
    digital_footprint = relationship("DigitalFootprint", back_populates="users")
    user = relationship("User", back_populates="digital_footprints")

    def __repr__(self):
        return (f"<UserDigitalFootprint(digital_footprint_id={self.digital_footprint_id}, "
                f"user_id={self.user_id})>")


class ActivityLog(Base):
    __tablename__ = 'activity_logs'

    # Columns
    digital_footprint_id = Column(Integer, ForeignKey('digital_footprints.id'), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)

    # Relationship
    digital_footprint = relationship("DigitalFootprint", back_populates='activity_logs')

    def __repr__(self):
        return (f"<ActivityLog(digital_footprint_id={self.digital_footprint_id}, "
                f"timestamp='{self.timestamp}')>")


class Source(Base):
    __tablename__ = 'sources'

    # Columns
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    url = Column(String(500), nullable=False)
    category = Column(Enum(SourceCategory), nullable=False)
    verified = Column(Boolean, nullable=False)

    # Relationship
    digital_footprints = relationship(
        "DigitalFootprint",
        back_populates="source",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Source(id={self.id}, name='{self.name}', url='{self.url}', category='{self.category}', verified={self.verified})>"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'name': self.name,
            'url': self.url,
            'category': self.category if self.category else None,
            'verified': self.verified,
            'digital_footprints': [
                {
                    'id': df.id,
                    'type': df.type if df.type else None,
                    'media_filepath': df.media_filepath,
                    'reference_url': df.reference_url,
                    'source_id': df.source_id
                }
                for df in self.digital_footprints
            ]
        }
