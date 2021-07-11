class Log4J:
    def __init__(self, spark):
        # Use your organisation name as the root_class i.e. guru...
        # guru.learning.. is the application logger name set in log4j properties file
        root_class = "guru.learningjournal.spark.examples"

        # use root class and append it with application name
        # self.logger = log4j.LogManager.getLogger(root_class + "." + "HelloSpark")
        conf = spark.sparkContext.getConf()

        # Following will bring "HelloSpark" from spark session .appName from HelloSpark.py file
        app_name = conf.get("spark.app.name")

        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)

