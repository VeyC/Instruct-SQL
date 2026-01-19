
# 用来转换agent生成的结果，用于测试

import json
import os



def fill_data():
    "有时候存在中断，需要整合到原来的记录中，先只整合json，没有整合log"
    origin_directory = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/schema_linking+schema_linking_info+sql_generation+sql_style_refinement+sql_output_refinement+sql_correction+sql_selection+sql_post_process/bird/2025-11-11-23-03-04'
    save_directory = './results/dev/schema_linking+schema_linking_info+sql_generation+sql_style_refinement+sql_output_refinement+sql_correction+sql_selection/bird/2025-11-08-23-21-42'

    ids = []
    for file in os.listdir(origin_directory):
        if file.endswith(".json") and "_" in file:
            print(file)
            _index = file.find("_")
            question_id = int(file[:_index])
            if os.path.exists(os.path.join(save_directory, file)):
                with open(os.path.join(save_directory, file), 'w', encoding='utf-8') as save_f, \
                    open(os.path.join(origin_directory, file), 'r', encoding='utf-8') as origin_f:
                    item = json.load(origin_f)
                    json.dump(item, save_f, indent=2, ensure_ascii=False)
                    ids.append(question_id)
    print(ids)
    print(len(ids))


# fill_data()

def change_to_evaluate_file():
    result_directorys = ['/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/sql_generation+sql_style_refinement+sql_output_refinement+sql_selection/bird/2025-12-21-12-22-28',
    '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/sql_generation+sql_style_refinement+sql_output_refinement+sql_selection/bird/2025-12-19-21-49-53',
    '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/sql_generation+sql_style_refinement+sql_output_refinement+sql_selection/bird/2025-12-20-14-44-33',
    '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/sql_generation+sql_style_refinement+sql_output_refinement+sql_selection/bird/2025-12-19-14-47-04',
    '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/sql_generation+sql_style_refinement+sql_output_refinement+sql_selection/bird/2025-12-18-10-17-22']
    # result_directory_2 = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/sql_generation+sql_style_refinement+sql_output_refinement+sql_selection/bird/2025-12-20-14-44-33'
    # result_directory_3 = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/sql_generation+sql_style_refinement+sql_output_refinement+sql_selection/bird/2025-12-04-11-17-48'
    # result_directory_4 = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/schema_linking_info/bird/2025-11-15-23-26-27'

    # result_directory = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/sql_selection/bird/2025-11-14-17-57-01'
    # save_path = '../output/bird/dev/agent_part.json'

    # result_directory = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/schema_linking+schema_linking_info/bird/2025-11-16-21-24-21'
    # result_directory_2 = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/schema_linking+schema_linking_info/bird/2025-11-17-11-09-51'
    # result_directory_3 = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/sql_generation/bird/2025-11-17-16-13-45'
    # result_directory_4 = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/sql_generation/bird/2025-11-17-17-16-20'

    # result_directory_2 = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/schema_linking+schema_linking_info/bird/2025-11-19-10-53-25'

    save_path = '../output/bird/dev/agent_test.json'

    # test_ids = [63, 90, 431, 514, 1250, 1276, 1300, 1391, 1486, 10, 79, 135, 194, 239, 251, 292, 361, 363, 377, 391, 403, 405, 449, 587, 593, 687, 711, 791, 849, 852, 855, 871, 921, 964, 984, 988, 1024, 1028, 1041, 1157, 1160, 1167, 1177, 1189, 1212, 1217, 1261, 1285, 1407, 1421, 1442, 1520]
    # test_ids = [90, 587, 871, 964, 988, 1041, 1285, 1300, 1486]
    # test_ids = [16,130,150,377,405,408,473,1189,1245,1264,1274,1265,1275,1273,1277,1271]
    # test_ids = [43, 80, 84, 85, 88, 102, 142, 173, 180, 217, 223, 311, 326, 384, 390, 411, 531, 594, 639, 640, 656, 685, 726, 772, 805, 812, 861, 889, 903, 913, 937, 948, 952, 957, 1015, 1175, 1216, 1406, 1419, 1422, 1433, 1436, 1450, 1451, 1453, 1456, 1458, 1476, 1477, 1498, 1517]
    # test_ids = [6, 7, 61, 64, 66, 183, 241, 288, 397, 440, 557, 674, 733, 773, 827, 850, 1053, 1138, 1238, 1460]
    # test_ids = [17, 23, 24, 25, 26, 27, 28, 31, 32, 36, 37, 41, 46, 47, 50, 72, 82, 83, 85, 87, 94, 95, 98, 99, 115, 116, 118, 125, 128, 129, 145, 149, 152, 159, 168, 169, 173, 186, 189, 197, 198, 201, 207, 215, 218, 219, 220, 231, 234, 243, 244, 245, 247, 248, 249, 263, 268, 273, 281, 340, 341, 344, 347, 349, 352, 371, 383, 402, 407, 412, 416, 424, 459, 462, 465, 469, 474, 483, 484, 486, 529, 530, 531, 533, 539, 563, 565, 571, 576, 581, 584, 586, 592, 595, 634, 637, 639, 640, 665, 671, 672, 682, 683, 685, 694, 707, 710, 716, 726, 728, 736, 743, 750, 751, 760, 766, 772, 788, 794, 798, 800, 801, 846, 847, 854, 861, 865, 866, 872, 877, 879, 881, 892, 894, 896, 898, 902, 904, 906, 915, 928, 930, 937, 944, 948, 950, 951, 954, 955, 959, 962, 963, 967, 972, 978, 989, 994, 1001, 1003, 1011, 1014, 1029, 1031, 1032, 1035, 1036, 1037, 1040, 1042, 1058, 1068, 1078, 1079, 1080, 1088, 1092, 1094, 1107, 1113, 1115, 1122, 1124, 1130, 1133, 1134, 1135, 1136, 1141, 1144, 1145, 1148, 1149, 1150, 1152, 1155, 1166, 1168, 1169, 1171, 1175, 1179, 1185, 1198, 1201, 1205, 1209, 1220, 1225, 1227, 1232, 1235, 1239, 1241, 1242, 1243, 1247, 1251, 1252, 1254, 1255, 1256, 1267, 1302, 1322, 1334, 1338, 1350, 1375, 1378, 1381, 1387, 1389, 1392, 1399, 1404, 1405, 1410, 1422, 1427, 1457, 1464, 1472, 1473, 1476, 1479, 1480, 1481, 1482, 1490, 1498, 1500, 1501, 1505, 1506, 1507, 1525, 1526, 1529, 1531, 1533]
    # test_ids = [17, 26, 27, 28, 36, 41, 72, 79, 83, 85, 87, 115, 125, 129, 145, 168, 169, 173, 186, 194, 197, 198, 207, 215, 234, 239, 243, 263, 268, 281, 341, 344, 349, 352, 371, 391, 412, 424, 465, 469, 473, 484, 529, 530, 531, 571, 581, 584, 587, 595, 634, 639, 640, 682, 683, 685, 694, 728, 743, 772, 791, 847, 861, 866, 877, 879, 892, 898, 930, 944, 948, 955, 963, 988, 989, 1011, 1014, 1028, 1058, 1107, 1133, 1135, 1144, 1148, 1152, 1157, 1166, 1168, 1185, 1205, 1235, 1239, 1241, 1242, 1247, 1254, 1255, 1257, 1265, 1275, 1322, 1399, 1404, 1422, 1473, 1481, 1482, 1486, 1490, 1501, 1525, 1529, 1531]
    # test_ids = [12, 23, 31, 32, 37, 40, 46, 47, 50, 77, 82, 92, 94, 95, 98, 99, 116, 128, 136, 149, 152, 159, 189, 201, 220, 227, 244, 245, 248, 249, 253, 273, 282, 347, 366, 377, 402, 405, 407, 416, 459, 462, 474, 483, 486, 539, 563, 565, 576, 592, 665, 671, 672, 707, 716, 736, 760, 766, 794, 798, 800, 801, 865, 872, 875, 881, 884, 894, 897, 904, 906, 915, 954, 959, 962, 964, 967, 978, 990, 994, 1001, 1002, 1003, 1031, 1032, 1040, 1042, 1068, 1076, 1078, 1079, 1080, 1110, 1115, 1136, 1145, 1149, 1150, 1171, 1175, 1189, 1227, 1252, 1256, 1267, 1302, 1323, 1334, 1338, 1350, 1375, 1381, 1387, 1389, 1392, 1410, 1457, 1460, 1464, 1472, 1479, 1480, 1500, 1505, 1524, 1526, 1533]
    # test_ids = [5, 17, 23, 25, 82, 85, 94, 112, 118, 125, 136, 145, 197, 207, 218, 219, 220, 231, 244, 281, 344, 352, 368, 371, 377, 405, 414, 416, 486, 522, 533, 565, 571, 573, 581, 672, 683, 687, 710, 736, 744, 766, 798, 861, 872, 875, 877, 879, 884, 896, 898, 930, 940, 988, 1011, 1042, 1044, 1068, 1079, 1094, 1102, 1115, 1130, 1168, 1179, 1239, 1243, 1251, 1256, 1404, 1422, 1427, 1464, 1479, 1482, 1483, 1484, 1493, 1498, 1521]
    # test_ids = [12, 31, 32, 40, 94, 282, 402, 565, 592, 672, 915, 1001, 1032, 1040, 1175, 1472, 1526] + [5, 11, 24, 25, 39, 45, 48, 62, 89, 93, 100, 112, 117, 118, 119, 120, 137, 138, 192, 195, 200, 206, 208, 212, 213, 218, 219, 226, 228, 230, 231, 232, 236, 240, 242, 247, 255, 260, 327, 340, 345, 346, 356, 358, 368, 379, 383, 397, 408, 409, 414, 415, 422, 427, 440, 466, 468, 472, 477, 479, 480, 487, 518, 522, 528, 532, 533, 537, 544, 547, 549, 555, 557, 567, 568, 572, 573, 578, 586, 598, 604, 629, 633, 637, 669, 678, 687, 701, 704, 705, 710, 717, 719, 723, 724, 726, 730, 732, 733, 737, 738, 739, 740, 744, 745, 747, 750, 751, 753, 758, 761, 764, 765, 769, 773, 775, 779, 781, 782, 785, 786, 788, 790, 792, 796, 797, 806, 819, 822, 824, 825, 829, 846, 850, 854, 857, 859, 862, 868, 869, 880, 895, 896, 901, 902, 909, 910, 912, 928, 931, 933, 937, 940, 945, 950, 951, 960, 971, 972, 977, 981, 1025, 1029, 1030, 1035, 1036, 1037, 1039, 1044, 1048, 1057, 1084, 1088, 1091, 1092, 1094, 1096, 1098, 1102, 1103, 1105, 1113, 1114, 1116, 1122, 1124, 1130, 1134, 1139, 1141, 1146, 1147, 1153, 1155, 1156, 1162, 1164, 1169, 1179, 1187, 1192, 1195, 1198, 1201, 1208, 1209, 1220, 1225, 1229, 1231, 1232, 1238, 1243, 1251, 1270, 1281, 1312, 1317, 1331, 1339, 1340, 1344, 1346, 1351, 1352, 1356, 1357, 1359, 1361, 1362, 1368, 1371, 1376, 1378, 1380, 1390, 1394, 1398, 1401, 1403, 1405, 1409, 1411, 1426, 1427, 1432, 1435, 1471, 1476, 1483, 1484, 1493, 1498, 1506, 1507, 1509, 1514, 1515, 1521, 1528]

    # test_ids = sorted(test_ids)
    saves = []
    ids = []

    for result_directory in result_directorys:
        print(len(os.listdir(result_directory)))
        for file in os.listdir(result_directory):
            if file.endswith(".json") and "_" in file and '-' not in file:
                # print(file)
                flag = 0
                _index = file.find("_")
                question_id = int(file[:_index])
                db_id = file[_index + 1:-5]
                # if question_id not in test_ids:
                #     continue
                with open(os.path.join(result_directory, file), 'r') as f:
                    exec_history = json.load(f)
                    item = {"question_id": question_id,
                            "db_id": db_id}
                    for step in exec_history:
                        if step['status'] == 'success':
                            if 'sql' in step:
                                item[step['node_type']] = step['sql']
                            elif 'sqls' in step:
                                item[step['node_type']] = step['sqls']
                        else:
                            item[step['node_type']] = ""
                            flag =1
                if flag:
                    ids.append(question_id)

                saves.append(item)
    
    saves = sorted(saves, key=lambda x: x['question_id'])
    
    print([item['question_id'] for item in saves])

    # print(len(os.listdir(result_directory_2)))
    # for file in os.listdir(result_directory_2):
    #     if file.endswith(".json") and "_" in file and '-' not in file:
    #         # print(file)
    #         flag = 0
    #         _index = file.find("_")
    #         question_id = int(file[:_index])                
    #         db_id = file[_index + 1:-5]
    #         with open(os.path.join(result_directory_2, file), 'r') as f:
    #             exec_history = json.load(f)
    #             item = {"question_id": question_id,
    #                     "db_id": db_id}
    #             for step in exec_history:
    #                 if step['status'] == 'success':
    #                     if 'sql' in step:
    #                         item[step['node_type']] = step['sql']
    #                     elif 'sqls' in step:
    #                         item[step['node_type']] = step['sqls']
    #                 else:
    #                     item[step['node_type']] = ""
    #                     flag =1
    #         if flag:
    #             ids.append(question_id)

    #         saves.append(item)
    

    # print(len(os.listdir(result_directory_3)))
    # for file in os.listdir(result_directory_3):
    #     if file.endswith(".json") and "_" in file and '-' not in file:
    #         # print(file)
    #         flag = 0
    #         _index = file.find("_")
    #         question_id = int(file[:_index])            
    #         if question_id in [12, 31, 32, 40, 94, 282, 402, 565, 592, 672, 915, 1001, 1032, 1040, 1175, 1472, 1526]:
    #             continue    
    #         db_id = file[_index + 1:-5]
    #         with open(os.path.join(result_directory_3, file), 'r') as f:
    #             exec_history = json.load(f)
    #             item = {"question_id": question_id,
    #                     "db_id": db_id}
    #             for step in exec_history:
    #                 if step['status'] == 'success':
    #                     if 'sql' in step:
    #                         item[step['node_type']] = step['sql']
    #                     elif 'sqls' in step:
    #                         item[step['node_type']] = step['sqls']
    #                 else:
    #                     item[step['node_type']] = ""
    #                     flag =1
    #         if flag:
    #             ids.append(question_id)

    #         saves.append(item)
    
    print('======================')
    print(len(saves))
    

    # for item in saves:
    #     path = f"{item['question_id']}_{item['db_id']}.json"
    #     if path in os.listdir(result_directory_2):
    #         # print(path)
    #         with open(os.path.join(result_directory_2, path), 'r') as f:
    #             exec_history = json.load(f)
    #             item['schema_linking'] = item['schema_linking'] + exec_history[0]['sqls']
    #             # item['schema_linking_info'] = item['schema_linking_info'] + exec_history[1]['sqls']
    #             item['schema_linking_info'] = exec_history[1]['sqls']
                # if exec_history[2]['status']=="success" and len(exec_history[2]['sqls']) == 1:
                #     # consistency_ids.append(item['question_id'])
                #     pass
                # elif exec_history[2]['status']=="success":
                #     item['sql_generation'] = item['sql_generation'] + exec_history[2]['sqls']
                    # consistency_ids.append(item['question_id'])
    
    # for item in saves:
    #     path = f"{item['question_id']}_{item['db_id']}.json"
    #     if path in os.listdir(result_directory_3):
    #         # print(path)
    #         with open(os.path.join(result_directory_3, path), 'r') as f:
    #             exec_history = json.load(f)
                
    #             item['sql_generation'] = exec_history[0]['sqls']
    
    # for item in saves:
    #     path = f"{item['question_id']}_{item['db_id']}.json"
    #     if path in os.listdir(result_directory_4):
    #         # print(path)
    #         with open(os.path.join(result_directory_4, path), 'r') as f:
    #             exec_history = json.load(f)
    #             try:
    #                 item['sql_generation'] = exec_history[0]['sqls']
    #             except Exception as e:
    #                 print(path)
    #     if path in os.listdir(result_directory_4):
    #         # print(path)
    #         with open(os.path.join(result_directory_4, path), 'r') as f:
    #             exec_history = json.load(f)
                
    #             item['schema_linking_info'] = item['schema_linking_info'] + exec_history[0]['sqls']
    #             consistency_ids.append(item['question_id'])

    # print (len(saves[0]['schema_linking_info']))

    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(saves, f, indent=2, ensure_ascii=False)

    print('finish saving !!')
    print(ids)
    print(len(ids))


def add_arctic():
    ours_path = '../output/bird/dev/agent_qwen.json'
    arctic_path = '/media/hnu/hnu2024/wangqin/python_work/ArcticTraining/projects/arctic_text2sql_r1/results/_media_hnu_LLM_Arctic-Text2SQL-R1-7B_dev_bird/greedy_search_base_filter_arctic.json'
    
    with open(arctic_path, 'r', encoding='utf-8') as f:
        datas = json.load(f)
    
    save_datas = []
    with open(ours_path, 'r', encoding='utf-8') as f:
        our_datas = json.load(f)
        for data in our_datas:
            question_id = data['question_id']
            data['arctic_sql'] = [datas[question_id]['pred_sql']]
            save_datas.append(data)
    
    with open(ours_path, 'w', encoding='utf-8') as f:
        json.dump(save_datas, f, indent=2, ensure_ascii=False)
    
    print(len(save_datas))
    print('finish saving file')

    

def merge_all_sample():
    result_path = ['../output/bird/dev/agent_510.json', '../output/bird/dev/agent_720.json', '../output/bird/dev/agent_eight.json']
    save_path = '../output/bird/dev/agent_1534.json'

    saves = []
    for path in result_path:
        with open(path,'r', encoding='utf-8') as f:
            datas = json.load(f)
            for data in datas:
                try:
                    del data['sql_style_refinement']
                    del data['sql_output_refinement']
                    del data['sql_correction']
                    del data['sql_selection']
                    del data['sql_post_process']
                except:
                    pass
                while len(data['schema_linking']) != 4:
                    data['schema_linking'].append(data['schema_linking'][-1]) 
                while len(data['schema_linking_info']) != 4:
                    data['schema_linking_info'].append(data['schema_linking_info'][-1]) 
                try:
                    if len(data['sql_generation']) > 1: 
                        while len(data['sql_generation']) != 8:
                            data['sql_generation'].append(data['sql_generation'][-1]) 
                    else:
                        data['sql_generation'] = data['schema_linking']+data['schema_linking_info']
                except:
                    data['sql_generation'] = data['schema_linking']+data['schema_linking_info']
                saves.append(data)
    
    saves = sorted(saves, key=lambda x: x['question_id'])
    print(len(saves))
    assert len(saves)==1534

    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(saves, f, indent=2, ensure_ascii=False)
    
    print('finish saving file!')


def merge_all_phrase():
    
    path = '../output/bird/dev/agent_1534.json'
    result_directory = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/src/results/dev/sql_output_refinement/bird/2025-11-23-22-54-17'
    # test_ids = [4, 10, 15, 19, 36, 40, 42, 43, 51, 54, 63, 79, 84, 86, 88, 90, 92, 102, 142, 172, 173, 180, 194, 217, 239, 251, 268, 284, 311, 326, 361, 363, 377, 384, 389, 390, 391, 401, 405, 410, 411, 429, 431, 449, 453, 463, 481, 494, 514, 556, 581, 582, 587, 590, 593, 594, 618, 639, 640, 653, 656, 685, 687, 726, 736, 772, 791, 794, 805, 812, 849, 852, 855, 861, 871, 889, 897, 903, 913, 915, 921, 922, 924, 937, 943, 948, 952, 957, 964, 984, 988, 992, 1002, 1024, 1027, 1028, 1041, 1063, 1068, 1078, 1113, 1157, 1160, 1172, 1175, 1177, 1178, 1189, 1212, 1213, 1217, 1226, 1229, 1231, 1250, 1253, 1257, 1261, 1262, 1264, 1276, 1277, 1282, 1285, 1290, 1300, 1314, 1334, 1381, 1383, 1385, 1387, 1391, 1393, 1410, 1419, 1421, 1422, 1433, 1436, 1442, 1450, 1451, 1452, 1453, 1456, 1457, 1458, 1466, 1477, 1486, 1498, 1503, 1517, 1520] 
    # test_ids = [16, 17, 25, 26, 27, 28, 33, 41, 49, 53, 65, 72, 101, 109, 110, 115, 124, 129, 130, 141, 144, 145, 168, 169, 171, 179, 185, 186, 193, 197, 198, 201, 207, 214, 215, 218, 219, 221, 234, 237, 244, 247, 248, 252, 254, 259, 263, 267, 269, 271, 281, 286, 290, 296, 298, 309, 310, 317, 328, 330, 335, 337, 338, 340, 341, 342, 343, 344, 349, 352, 354, 357, 359, 360, 371, 376, 386, 387, 388, 392, 398, 399, 406, 408, 412, 417, 424, 425, 428, 432, 433, 437, 441, 442, 443, 444, 446, 447, 448, 454, 458, 465, 469, 473, 476, 482, 484, 499, 500, 507, 515, 517, 519, 523, 529, 530, 533, 571, 584, 586, 595, 596, 599, 600, 602, 603, 608, 610, 616, 628, 630, 631, 632, 635, 642, 646, 649, 667, 679, 682, 683, 686, 689, 692, 693, 694, 696, 709, 710, 720, 728, 741, 743, 766, 788, 802, 810, 832, 847, 851, 866, 879, 891, 892, 893, 896, 902, 905, 908, 927, 928, 930, 944, 950, 951, 953, 955, 958, 963, 966, 970, 973, 974, 975, 979, 985, 986, 987, 995, 996, 998, 1000, 1004, 1006, 1010, 1011, 1012, 1014, 1023, 1026, 1029, 1034, 1058, 1061, 1064, 1085, 1092, 1093, 1094, 1107, 1118, 1119, 1120, 1121, 1126, 1127, 1131, 1133, 1135, 1144, 1148, 1152, 1166, 1168, 1170, 1179, 1185, 1186, 1197, 1199, 1200, 1204, 1205, 1211, 1219, 1223, 1225, 1233, 1239, 1241, 1242, 1243, 1245, 1247, 1248, 1251, 1254, 1255, 1260, 1265, 1269, 1271, 1273, 1274, 1275, 1279, 1284, 1308, 1318, 1322, 1365, 1370, 1388, 1389, 1399, 1404, 1418, 1427, 1481, 1482, 1491, 1496, 1500, 1501, 1525, 1529, 1530, 1531]
    # test_ids = [17, 22, 30, 41, 46, 50, 78, 80, 81, 83, 85, 87, 88, 125, 155, 159, 165, 211, 223, 231, 243, 283, 292, 303, 366, 402, 403, 430, 445, 459, 531, 634, 726, 728, 865, 877, 888, 894, 898, 911, 989, 993, 1000, 1004, 1009, 1015, 1021, 1078, 1145, 1149, 1158, 1216, 1225, 1235, 1338, 1366, 1406, 1407, 1437, 1473, 1476, 1533]
    # test_ids = [4, 76, 243, 284, 292, 400, 401, 410, 474, 481, 486, 525, 625, 655, 671, 736, 763, 842, 873, 907, 916, 1137, 1158, 1177, 1236, 1276, 1282, 1296, 1450, 1452, 1474]
    # test_ids = [83, 87, 94, 152, 402, 511, 520, 652, 672, 878, 956, 959, 967, 976, 1005, 1085, 1218, 1234, 1249, 1292]

    saves = []
    ids = []

    with open(path, 'r', encoding='utf-8') as f:
        saves = json.load(f)

    for file in os.listdir(result_directory):
        if file.endswith(".json") and "_" in file:
            # print(file)
            _index = file.find("_")
            question_id = int(file[:_index])
            db_id = file[_index + 1:-5]
            if question_id not in test_ids:
                continue
            with open(os.path.join(result_directory, file), 'r') as f:
                exec_history = json.load(f)
                try:
                    # if len(exec_history[0]['sqls']) == 8:
                    #     saves[question_id]['sql_style_refinement'] = exec_history[0]['sqls']
                    #     ids.append(question_id)
                    # else:
                    #     print(question_id)
                    saves[question_id]['sql_output_refinement'] = exec_history[0]['sqls']
                except Exception as e:
                    print(e)
                    print(question_id)

    # for i, data in enumerate(saves):
    #     if data.get('sql_output_refinement', 'no') == 'no':
    #         saves[i]['sql_output_refinement'] = saves[i]['sql_selection']

    print(len(ids))
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(saves, f, indent=2, ensure_ascii=False)

    print('finish saving file !')

# 这里先拿来直接用了，后续还需要直接嵌入到流程中
def merge_example():
    file1 = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/output/bird/dev/dev_bird_metadata.json'
    file2 = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/output/bird/dev/dev_bird_metadata copy.json'

    with open(file1, 'r', encoding='utf-8') as f:
        data1 = json.load(f)
    
    with open(file2, 'r', encoding='utf-8') as f:
        data2 = json.load(f)
        for i, item in enumerate(data2):
            data1[i]['example'] = data2[i]['example']
    
    with open(file1, 'w', encoding='utf-8') as f:
        json.dump(data1, f, indent=2, ensure_ascii=False)
    
    print('finish change')


def merge_arctic():
    path = '../output/bird/dev/agent_1534_vote.json'
    arctic_path = '/media/hnu/hnu2024/wangqin/python_work/ArcticTraining/projects/arctic_text2sql_r1/results/_media_hnu_LLM_Arctic-Text2SQL-R1-7B_dev_bird/major_voting_base_old.json'
    save_path = '../output/bird/dev/agent_1534_vote.json'

    with open(arctic_path, 'r', encoding='utf-8') as f:
        arctic_datas = json.load(f)
    
    with open(path, 'r', encoding='utf-8') as f:
        datas = json.load(f)
        for i, data in enumerate(datas):
            datas[i]['arctic_sql_all'] = arctic_datas[i]['pred_sqls']

    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(datas, f, indent=2, ensure_ascii=False)

    print('finish saving file !!')



def add_schema_linking():
    origin_file = "/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/output/bird/dev/agent_1534.json"
    save_file = "/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/output/bird/dev/agent_test.json"

    with open(origin_file, 'r', encoding='utf-8') as f:
        origin_data = json.load(f)
    
    with open(save_file, 'r', encoding='utf-8') as f:
        save_datas = json.load(f)

    for i, data in enumerate(save_datas):
        id = data['question_id']
        print(origin_data[id]['question_id'], save_datas[i]['question_id'])
        assert origin_data[id]['question_id'] == save_datas[i]['question_id']
        save_datas[i]['schema_linking'] = [save_datas[i]['sql_generation'][0], origin_data[id]['schema_linking'][0], origin_data[id]['schema_linking'][2], origin_data[id]['schema_linking_info'][0], origin_data[id]['schema_linking_info'][2]]
    
    with open(save_file, 'w', encoding='utf-8') as f:
        json.dump(save_datas, f, indent=2, ensure_ascii=False)
    
    print('finish saving file !')


# change_to_evaluate_file()
add_arctic()
# merge_all_sample()
# merge_all_phrase()
# merge_example()
# merge_arctic()
# add_schema_linking()