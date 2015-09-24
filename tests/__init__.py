from mortar_rdb.testing import register_session

def setup_module():
    register_session(transactional=False)
