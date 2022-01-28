import boto3

# テーブル操作
class Table:
    # コンストラクタ
    def __init__(self):
        self.dynamodb_client = boto3.client('dynamodb', region_name='us-west-2')
        self.dynamodb_resource = boto3.resource('dynamodb',  region_name='us-west-2')
        self.table_name = 'Fujiwara_Employee'

    # テーブル作成
    def create_table(self):
        print('create table start')

        # テーブルの存在チェック
        if self.is_exists_table():
            print('table is exists')
            return

        # テーブル作成
        create_table_arg = {
            'TableName': self.table_name,
            'AttributeDefinitions': [
                # 会社コード
                {
                    'AttributeName': 'CompanyCode',
                    'AttributeType': 'S'
                },
                # 社員番号
                {
                    'AttributeName': 'EmployeeNumber',
                    'AttributeType': 'S'
                },
                # 部署ID
                {
                    'AttributeName': 'DepartmentId',
                    'AttributeType': 'S'
                }
            ],
            'KeySchema': [
                {
                    'AttributeName': 'CompanyCode',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'EmployeeNumber',
                    'KeyType': 'RANGE'
                }
            ],
            'LocalSecondaryIndexes': [
                {
                    'IndexName': 'CompanyCodeDepartmentIdIndex',
                    'KeySchema': [
                        {
                            'AttributeName': 'CompanyCode',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'DepartmentId',
                            'KeyType': 'RANGE'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    }
                },
            ],
            'GlobalSecondaryIndexes': [
                {
                    'IndexName': 'DepartmentIdIndex',
                    'KeySchema': [
                        {
                            'AttributeName': 'DepartmentId',
                            'KeyType': 'HASH'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    }
                }
            ],
            'BillingMode': 'PAY_PER_REQUEST'
        }
        self.dynamodb_resource.create_table(**create_table_arg)

        print('create table success')

    # テーブル削除
    def drop_table(self):
        print('drop table start')

        # テーブルの存在チェック
        if not self.is_exists_table():
            print('table is not exists')
            return

        # テーブル削除
        self.dynamodb_resource.Table(self.table_name).delete()

        print('drop table success')

    def is_exists_table(self):
        try:
            self.dynamodb_client.describe_table(TableName=self.table_name)

            # テーブルが存在する
            return True
        except self.dynamodb_client.exceptions.ResourceNotFoundException:
            # テーブルが存在しない
            return False

# 実行
table = Table()
table.create_table()
#table.drop_table()
