from pydantic import BaseModel
from datetime import datetime

# ALL object attributes should have type annotations and default values.
# In most cases the default values will be None, but you can set them to anything you want.

class Sample_User(BaseModel):
    username: str = None
    user_discriminator: int = None
    user_last_action: str = None

    def quote(self): # This was originally based on discord API, other purposes might have similar usage
        return f"{self.username}#{self.user_discriminator}"

    def too_fast(self):
        if self.user_last_action == None:
            return True
        last_call = datetime.fromisoformat(self.user_last_action)
        if (datetime.now() - last_call).total_seconds() >= 30:
            return True
        return False

class Sample_Admin(Sample_User):
    credentials: int = None

class Sample_Member(Sample_User):
    warnings: int = 0