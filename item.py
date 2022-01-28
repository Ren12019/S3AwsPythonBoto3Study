import boto3

# 項目操作
class Item:
    # コンストラクタ
    def __init__(self):
        # self.dynamodb_client = boto3.client('dynamodb', region_name='us-west-2')
        self.dynamodb_resource = boto3.resource('dynamodb',  region_name='us-west-2')
        self.table = self.dynamodb_resource.Table('Fujiwara_Employee')

    # 全件削除
    def delete_item_all(self):
        # 全件取得
        employees = self.table.scan()

        # 全件削除
        with self.table.batch_writer() as batch:
            for item in employees['Items']:
                batch.delete_item(
                    Key={
                        'CompanyCode': item['CompanyCode'],
                        'EmployeeNumber': item['EmployeeNumber']
                    }
                )

    # 登録
    def put_item(self):
        # 1件登録
        self.table.put_item(
            Item={
                'CompanyCode': '001',
                'EmployeeNumber': '00000001',
                'DepartmentId': 'D001',
                'EmployeeName': '山田一郎',
                'JoinDate': '2000-04-01'
            }
        )

        # 複数件登録
        items = [
            {
                'CompanyCode': '001',
                'EmployeeNumber': '00000002',
                'DepartmentId': 'D001',
                'EmployeeName': '山田二郎',
                'JoinDate': '2000-04-01'
            },
            {
                'CompanyCode': '001',
                'EmployeeNumber': '00000003',
                'DepartmentId': 'D002',
                'EmployeeName': '山田三郎',
                'JoinDate': '2000-04-01'
            },
            {
                'CompanyCode': '002',
                'EmployeeNumber': '00000001',
                'DepartmentId': 'D001',
                'EmployeeName': '山田四郎',
                'JoinDate': '2000-04-01'
            }
        ]
        with self.table.batch_writer() as batch:
            for item in items:
                batch.put_item(
                    Item={
                        'CompanyCode': item['CompanyCode'],
                        'EmployeeNumber': item['EmployeeNumber'],
                        'DepartmentId': item['DepartmentId'],
                        'EmployeeName': item['EmployeeName'],
                        'JoinDate': item['JoinDate']
                    }
                )

    # 参照
    def get_item(self):
        # PKで取得
        employee = self.table.get_item(
            Key={
                'CompanyCode': '001',
                'EmployeeNumber': '00000001'
            }
        )
        print('■PKで取得')
        print(f"  CompanyCode={employee['Item']['CompanyCode']}");
        print(f"  EmployeeNumber={employee['Item']['EmployeeNumber']}")
        print(f"  DepartmentId={employee['Item']['DepartmentId']}")
        print(f"  EmployeeName={employee['Item']['EmployeeName']}")
        print(f"  JoinDate={employee['Item']['JoinDate']}")

        # LSIで取得
        employeesLsi = self.table.query(
            KeyConditions={
                'CompanyCode': {
                    'AttributeValueList': ['001'],
                    'ComparisonOperator': 'EQ'
                },
                'DepartmentId': {
                    'AttributeValueList': ['D001'],
                    'ComparisonOperator': 'EQ'
                }
            },
            IndexName='CompanyCodeDepartmentIdIndex'
        )
        print('■LSIで取得')
        count = 1
        for employee in employeesLsi['Items']:
            print(f'  {count}件目')
            print(f"    CompanyCode={employee['CompanyCode']}");
            print(f"    EmployeeNumber={employee['EmployeeNumber']}")
            print(f"    DepartmentId={employee['DepartmentId']}")
            print(f"    EmployeeName={employee['EmployeeName']}")
            print(f"    JoinDate={employee['JoinDate']}")
            count += 1

        # GSIで取得
        employeesGsi = self.table.query(
            KeyConditions={
                'DepartmentId': {
                    'AttributeValueList': ['D001'],
                    'ComparisonOperator': 'EQ'
                }
            },
            IndexName='DepartmentIdIndex'
        )
        print('■GSIで取得')
        count = 1
        for employee in employeesGsi['Items']:
            print(f'  {count}件目')
            print(f"    CompanyCode={employee['CompanyCode']}");
            print(f"    EmployeeNumber={employee['EmployeeNumber']}")
            print(f"    DepartmentId={employee['DepartmentId']}")
            print(f"    EmployeeName={employee['EmployeeName']}")
            print(f"    JoinDate={employee['JoinDate']}")
            count += 1

    # 更新
    def update_item(self):
        # 1件更新
        self.table.update_item(
            Key={
                'CompanyCode': '001',
                'EmployeeNumber': '00000001'
            },
            AttributeUpdates={
                'DepartmentId': {
                    'Value': 'X001'
                },
                'EmployeeName': {
                    'Value': '田中一郎'
                },
                'JoinDate': {
                    'Value': '2022-04-01'
                }
            }
        )

        # batch_writerには複数件更新はない

    # 削除
    def delete_item(self):
        # 1件削除
        self.table.delete_item(
            Key={
                'CompanyCode': '001',
                'EmployeeNumber': '00000001'
            }
        )

        # 複数件削除
        items = [
            {
                'CompanyCode': '001',
                'EmployeeNumber': '00000002',
            },
            {
                'CompanyCode': '001',
                'EmployeeNumber': '00000003',
            },
            {
                'CompanyCode': '002',
                'EmployeeNumber': '00000001',
            }
        ]
        with self.table.batch_writer() as batch:
            for item in items:
                batch.delete_item(
                    Key={
                        'CompanyCode': item['CompanyCode'],
                        'EmployeeNumber': item['EmployeeNumber']
                    }
                )

# 実行
item = Item()
item.delete_item_all()
item.put_item()
item.get_item()
item.update_item()
item.delete_item()
