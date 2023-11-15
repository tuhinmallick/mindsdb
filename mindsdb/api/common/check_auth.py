import traceback


def check_auth(username, password, scramble_func, salt, company_id, config, logger=None):
    if logger is None:
        from mindsdb.api.mysql.mysql_proxy.utilities import logger

    try:
        hardcoded_user = config['auth'].get('username')
        hardcoded_password = config['auth'].get('password')
        if hardcoded_password is None:
            hardcoded_password = ''
        hardcoded_password_hash = scramble_func(hardcoded_password, salt)
        hardcoded_password = hardcoded_password.encode()

        if password is None:
            password = ''
        if isinstance(password, str):
            password = password.encode()

        if username != hardcoded_user:
            logger.warning(f'Check auth, user={username}: user mismatch')
            return {
                'success': False
            }

        if password not in [hardcoded_password, hardcoded_password_hash]:
            logger.warning(f'check auth, user={username}: password mismatch')
            return {
                'success': False
            }

        logger.info(f'Check auth, user={username}: Ok')
        return {
            'success': True,
            'username': username
        }
    except Exception as e:
        logger.error(f'Check auth, user={username}: ERROR')
        logger.error(e)
        logger.error(traceback.format_exc())
