from pathlib import Path

from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    sql = (Path(__file__).resolve().parents[2] / "infra/schema.sql").read_text()
    op.get_bind().connection.run_async(lambda connection: connection.execute(sql))


def downgrade():
    raise RuntimeError("Restore a database backup instead of deleting the event history")
