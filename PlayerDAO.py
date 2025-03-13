import cx_Oracle


class  Player:
    def __init__(self,id=None, name=None, age=None, weight=None, salary=None):
        self.id = id
        self.name = name
        self.age  = age
        self.weight = weight
        self.salary = salary

    def __str__(self):
        return f"Player[{self.id}] {self.name}, Age: {self.age}, Weight: {self.weight}kg, Salary: {self.salary} "


class  PlayerDAO:

    def __init__(self, dsn,user,password):
        ''' Oracle DB 연결 초기화'''
        self.dsn = dsn
        self.user = user
        self.password = password
        self.conn = None #DB 연결
        self.cursor = None #SQL 수행


    def connect(self):
        ''' DB연결 '''
        try:
            self.conn = cx_Oracle.connect(user=self.user, password = self.password,dsn =self.dsn)
            self.cursor = self.conn.cursor()
            print(f'DB 연결 성공:{self.cursor}')
        except cx_Oracle.DatabaseError as e:
            print(f'DB 연결 실패:{e}')

    def close(self):
        '''DB연결 종료'''
        if self.cursor:
            self.cursor.close()

        if self.conn:
            self.conn.close()
            print("DB 연결 종료!")

    def doSave(self, player):
        '''선수 추가 : insert '''
        try:
            self.connect()
            sql = '''
             INSERT INTO player (
                    name,
                    age,
                    weight,
                    salary
                ) VALUES ( :1,
                           :2,
                           :3,
                           :4 )
             '''
            print(f'1.sql:\n{sql}')
            print(f'2.param:{player}')
            #SQL Run
            self.cursor.execute(sql, (player.name, player.age,player.weight,player.salary)) # param tuple : (player.name,)
            self.conn.commit()
            print(f'doSave: 선수추가 성공!')
            #반영된 행 개수 출력
            print(f'반영된 행 개수:{self.cursor.rowcount}')
        except cx_Oracle.DatabaseError as e:
            self.conn.rollback()
            print(f'doSave() 실패:{e}')
        finally:
            self.close()

        return self.cursor.rowcount

    def doSelectOne(self,id):
        ''' 선수 단건 조회'''
        try:
            self.connect()
            sql = '''
                SELECT
                    id,
                    name,
                    age,
                    weight,
                    salary
                FROM
                    player
                WHERE id = :1            
            '''
            print(f'1.sql:\n{sql}')
            print(f'2.param: {id}')

            self.cursor.execute(sql,(id,))

            #단건조회
            row=self.cursor.fetchone()
            if row:
                print(f'row:{row}')
                return Player(id=row[0],name=row[1],age=row[2], weight=row[3],salary=row[4])

            return None

        except cx_Oracle.DatabaseError as e:
            print(f'doSelectOne 실패:{e}')
            return None
        finally:
            self.close()

    def doDelete(self,id):
        '''선수 삭제 : doDelete '''
        try:
            self.connect()
            sql = '''
                DELETE FROM player
                WHERE id = :1            
            '''
            print(f'1.sql:\n{sql}')
            print(f'2.param:{id}')

            flag=self.cursor.execute(sql, (id,))
            print(f'flag: {flag}')
            self.conn.commit()
            print(f'doDelete: 선수 삭제 성공!')
            print(f'반영된 행 개수:{self.cursor.rowcount}')
        except cx_Oracle.DatabaseError as e:
            self.conn.rollback()
            print(f'doSave() 실패:{e}')
        finally:
            self.close()

        return self.cursor.rowcount

    def doRetrieve(self):
        ''' 전체 조회'''
        try:
            self.connect()
            sql = """
            SELECT * FROM player
            """
            print(f'1 sql=\n{sql}')

            self.cursor.execute(sql)
            rows = self.cursor.fetchall()

            #리스트 내포
            return [Player(id=row[0],name=row[1],age=row[2], weight=row[3],salary=row[4]) for row in rows]

        except cx_Oracle.DatabaseError as e:
            print(f'doRetrieve: 조회 실패:{e}')
            return []
        finally:
            self.close()

    def doUpdate(self, player):
        ''' doUpdate player수정'''
        try:
            self.connect()
            sql = '''
                UPDATE player
                SET
                    name   = :1,
                    age    = :2,
                    weight = :3,
                    salary = :4
                WHERE
                        id = :5            
            '''
            print(f'1.sql:\n{sql}')
            print(f'2.param:{player}')

            self.cursor.execute(sql, (player.name, player.age,player.weight,player.salary,player.id)) # param tuple : (player.name,)
            self.conn.commit()
            print(f'doSave: 선수수정 성공!')
            print(f'반영된 행 개수:{self.cursor.rowcount}')
        except cx_Oracle.DatabaseError as e:
            self.conn.rollback()
            print(f'doSave() 실패:{e}')
        finally:
            self.close()

        return self.cursor.rowcount

def main():
    dsn = cx_Oracle.makedsn('192.168.100.245','1521',service_name='XE') #DB연결 정보
    user = 'scott'      #사용자 계정
    password = 'pcwk'   #비밀번호

    #DAO객체 생성
    dao=PlayerDAO(dsn,user,password)

    player01=Player(name='홍길동',age=22, weight=77.2,salary=8000000)
    flag=dao.doSave(player01)
    print(f'flag:{flag}')


    #단건 조회
    #outPlayer=dao.doSelectOne(1)
    #print(f'outPlayer:{outPlayer}')

    #단건 삭제
    flag=dao.doDelete(4)
    print(f'flag:{flag}')
    #목록 조회
    #players=dao.doRetrieve()

    #for vo in players:
    #    print(vo)


    #수정
    player01 = Player(id=1,name='홍길동_U', age=22+1, weight=77.2+5, salary=8000000+100)
    flag = dao.doUpdate(player01)
    print(f'flag:{flag}')

if __name__ == '__main__':
    main()

