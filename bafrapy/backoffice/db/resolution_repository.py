from sqlalchemy import select
from sqlalchemy.orm import Session

from bafrapy.backoffice.db.crud import CRUDRepository
from bafrapy.backoffice.models import Resolution


class ResolutionRepository(CRUDRepository[Resolution, str]):
    def __init__(self, session: Session):
        super().__init__(Resolution, session)

    def get_by_code(self, code: str) -> Resolution | None:
        return self.session.execute(select(Resolution).where(Resolution.code == code)).scalar_one_or_none()

    def get_by_seconds(self, seconds: int) -> Resolution | None:
        return self.session.execute(select(Resolution).where(Resolution.seconds == seconds)).scalar_one_or_none()
