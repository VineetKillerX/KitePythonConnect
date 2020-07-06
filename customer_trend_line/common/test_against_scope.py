import pandas as pd

def test_rule_scope(spark, test_file_path, test_file_seperator, test_on_column, dataframe, column_name, rule_name=''):
    try:
        test_data = spark.read.csv(test_file_path, sep=test_file_seperator, header=True).select(test_on_column).collect()
        test_data = set([data[test_on_column] for data in test_data])
        test_on_data = set([data[column_name] for data in dataframe.select(column_name).collect()])
        common_values = test_data.intersection(test_on_data)
        # case 1: data values in scope file not present in result
        case1 = test_data.difference(common_values)
        # case 2: data values in result but not in scope file
        case2 = test_on_data.difference(common_values)
        print("Number of data values in scope file not present in result")
        print(len(case1))
        print("Number of data values in result but not in scope file")
        print(len(case2))
        dataframe = pd.DataFrame(list(case1), columns=['Values in scope file'])
        dataframe.to_csv(rule_name+'ValuesInScopeFile.csv', index=False)
        dataframe = pd.DataFrame(list(case2), columns=['Values in result'])
        dataframe.to_csv(rule_name+'ValuesInResult.csv', index=False)
        del dataframe
    except Exception as e:
        print(e)