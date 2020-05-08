"""add market

Revision ID: 8589e76fe161
Revises: 5d1b2e83abe8
Create Date: 2020-05-07 12:32:02.809431

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8589e76fe161'
down_revision = '5d1b2e83abe8'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('trade', sa.Column('market', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('trade', 'market')
    # ### end Alembic commands ###