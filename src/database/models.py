from functools import partial

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

    def to_dict(self) -> dict:
        """Convert the User object and its relationships to a dictionary format."""
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

    def __repr__(self):
        return f"<DigitalFootprint(id={self.id}, type='{self.type}', media_filepath='{self.media_filepath}', reference_url='{self.reference_url}', source_id={self.source_id})>"


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
