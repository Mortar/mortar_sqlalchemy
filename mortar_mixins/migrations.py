# helpers for use in migrations

def migrate_temporal(table):
    # imports here so we don't have a dependency on alembic
    from alembic import op
    import sqlalchemy as sa
    from sqlalchemy.schema import Sequence, CreateSequence

    op.execute(CreateSequence(Sequence(table+"_id_seq")))
    op.add_column(table, sa.Column(
        'id', sa.Integer(), nullable=False,
        server_default=sa.text("nextval('"+table+"_id_seq'::regclass)")
    ))
    op.drop_constraint(table+'_pkey', table_name=table)
    op.create_primary_key(table+'_pkey', table, ['id'])
