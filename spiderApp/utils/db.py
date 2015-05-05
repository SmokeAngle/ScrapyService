import pymysql

#con = pymysql.connect(dsn='localhost:scrapy', user='root', password='')
#con = pymysql.Connection(host="localhost", user='root', password="", 
                          #database='scrapy', port=3306)
#cursor = con.cursor()
#cursor.execute('set names utf8')
#cursor.execute('select * from project_setting')
#print cursor.fetchall()


########################################################################
class dbUtils():
    """dbUtils"""
    host = '127.0.0.1'
    port = 3306
    user = 'root'
    password = ''
    db = 'scrapy'
    charset = 'utf8'
    __instance = None
    def __init__(self, host = None, port = None, user = None, password = None, db = None, charset=None):
        """Constructor"""
        host = host or self.host
        port = port or self.port
        user = user or self.user
        password = password or self.password
        db = db or self.db
        charset = charset or self.charset
        
        try:
            self.con = pymysql.Connection(host = host, port = port , user = user, password = password, database = db, cursorclass=pymysql.cursors.DictCursor)
            self.con.set_charset(charset)
            cursor = self.con.cursor()
            cursor.execute('SET CHARACTER SET %s' % charset)
            cursor.execute('SET character_set_connection=%s' % charset)
            cursor.execute('SET NAMES  %s' % charset)
        except pymysql.DatabaseError as conerr:
            self.con = None
            print conerr
            exit

    def execute(self, sql):
        cursor = self.con.cursor()
        cursor.execute(sql)
        self.con.commit()        
    
    def insert(self, sql):
        cursor = self.con.cursor()
        cursor.execute(sql)
        self.con.commit()
        return cursor.lastrowid
    def query(self, sql):
            cursor = self.con.cursor()
            cursor.execute(sql)
            return cursor.fetchall()
        
    def queryRow(self, sql):
        cursor = self.con.cursor()
        cursor.execute(sql)
        return cursor.fetchone()
    
    @classmethod
    def getInstance(cls):
        if cls.__instance == None:
            cls.__instance = dbUtils()
        return cls.__instance
    @classmethod
    def setCharset(cls, charset):
        cls.charset = charset
    
    def __exit__(self):
        self.con.close()
        self.con.cursor().close();