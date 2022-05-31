import pymysql
from elastalert.loaders import RulesLoader
from elastalert.util import elastalert_logger as log
import json


class MySqlRulesLoader(RulesLoader):
    """
    conf是 运行elastalert时传入的配置文件
    """
    def __init__(self, conf):
        super(MySqlRulesLoader, self).__init__(conf)
        self.cache = []
        self.config = conf
        self.table_name = self.config[conf['env']]['mysql']['tablename']
        self.sql = 'select id, rule_name, project, rule_content, update_time from %s' % self.table_name
        self.refresh()

    def refresh(self):
        env = self.config['env']
        self.db = pymysql.connect(host=self.config[env]['mysql']['host'],
                                  user=self.config[env]['mysql']['user'],
                                  password=self.config[env]['mysql']['password'],
                                  database=self.config[env]['mysql']['database'])
        self.cache = []
        cursor = self.db.cursor(pymysql.cursors.DictCursor)

        # 使用 execute()  方法执行 SQL 查询
        cursor.execute(self.sql)

        # 使用 fetchone() 方法获取单条数据.
        data = cursor.fetchall()
        for row in data:
            row['rule_content'] = json.loads(row['rule_content'])
            self.cache.append(row)
        log.info('refresh get rules: ' + self.cache.__str__())
        self.db.close()

    """
    返回规则文件的yml内容, elastalert.py会调用这个
    """
    def load(self, conf, args=None):
        """
        Discover and load all the rules as defined in the conf and args.
        :param dict conf: Configuration dict
        :param dict args: Arguments dict
        :return: List of rules
        :rtype: list
        """
        names = []
        rules = []
        for row in self.cache:
            if row['rule_name'] in names:
                log.info('dumplicate rule name %s', row['rule_name'])
            else:
                names.append(row['rule_name'])
                rules.append(row['rule_content'])
        log.info('return rules:' + rules.__str__())
        return rules

    """
    返回规则的名字
    """
    def get_names(self, conf, use_rule=None):
        """
        Return a list of rule names that can be passed to `get_yaml` to retrieve.
        :param dict conf: Configuration dict
        :param str use_rule: Limit to only specified rule
        :return: A list of rule names
        :rtype: list
        """
        names = []
        for row in self.cache:
            if row['rule_name'] in names:
                log.info('dumplicate rule name %s', row['rule_name'])
            else:
                names.append(row['rule_name'])
        log.info('return rule names:' + names.__str__())
        return names

    """
    elastalert用来判断是否有配置变更
    """
    def get_hashes(self, conf, use_rule=None):
        """
        Discover and get the hashes of all the rules as defined in the conf.
        :param dict conf: Configuration
        :param str use_rule: Limit to only specified rule
        :return: Dict of rule name to hash
        :rtype: dict
        """
        self.refresh()
        rule_times = {}
        for row in self.cache:
            rule_times[row['rule_name']] = row['update_time']
        log.info('return rule_times:' + rule_times.__str__())
        return rule_times

    """
    返回规则文件读取的yml内容
    """
    def get_yaml(self, filename):
        """
        Get and parse the yaml of the specified rule.
        :param str filename: Rule to get the yaml
        :return: Rule YAML dict
        :rtype: dict
        """
        for row in self.cache:
            if row['rule_name'] == filename:
                log.info('get rule from cache success, rule_name:%s' % filename)
                return row['rule_content']
        log.error('find rule from cache failed, rule_name:%s' % filename)
        return None
