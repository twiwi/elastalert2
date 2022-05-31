from pymongo import MongoClient
from elastalert.loaders import RulesLoader
import yaml

class MongoRulesLoader(RulesLoader):
    def __init__(self, conf):
        super(MongoRulesLoader, self).__init__(conf)
        self.client = MongoClient(conf['mongo_url'])
        self.db = self.client[conf['mongo_db']]
        self.cache = {}

    def get_names(self, conf, use_rule=None):
        """
        Return a list of rule names that can be passed to `get_yaml` to retrieve.
        :param dict conf: Configuration dict
        :param str use_rule: Limit to only specified rule
        :return: A list of rule names
        :rtype: list
        """
        if use_rule:
            return [use_rule]

        rules = []
        self.cache = {}
        for rule in self.db.rules.find():
            self.cache[rule['name']] = yaml.load(rule['yaml'])
            rules.append(rule['name'])

        return rules

    def get_hashes(self, conf, use_rule=None):
        """
        Discover and get the hashes of all the rules as defined in the conf.
        :param dict conf: Configuration
        :param str use_rule: Limit to only specified rule
        :return: Dict of rule name to hash
        :rtype: dict
        """
        if use_rule:
            return [use_rule]

        hashes = {}
        self.cache = {}
        for rule in self.db.rules.find():
            self.cache[rule['name']] = rule['yaml']
            hashes[rule['name']] = rule['hash']

        return hashes

    def get_yaml(self, rule):
        """
        Get and parse the yaml of the specified rule.
        :param str filename: Rule to get the yaml
        :return: Rule YAML dict
        :rtype: dict
        """
        if rule in self.cache:
            return self.cache[rule]

        self.cache[rule] = yaml.load(self.db.rules.find_one({'name': rule})['yaml'])
        return self.cache[rule]
