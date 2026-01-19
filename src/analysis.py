import json

def read_sql_file():
    file = '/media/hnu/hnu2024/wangqin/python_work/Text2SQL_submit/datasets/bird/dev/dev.sql'
    test_ids = [5, 11, 12, 17, 23, 24, 25, 26, 27, 28, 31, 32, 36, 37, 39, 40, 41, 45, 46, 47, 48, 50, 62, 72, 77, 79, 82, 83, 85, 87, 89, 92, 93, 94, 95, 98, 99, 100, 112, 115, 116, 117, 118, 119, 120, 125, 128, 129, 136, 137, 138, 145, 149, 152, 159, 168, 169, 173, 186, 189, 192, 194, 195, 197, 198, 200, 201, 206, 207, 208, 212, 213, 215, 218, 219, 220, 226, 227, 228, 230, 231, 232, 234, 236, 239, 240, 242, 243, 244, 245, 247, 248, 249, 253, 255, 260, 263, 268, 273, 281, 282, 327, 340, 341, 344, 345, 346, 347, 349, 352, 356, 358, 366, 368, 371, 377, 379, 383, 391, 397, 402, 405, 407, 408, 409, 412, 414, 415, 416, 422, 424, 427, 440, 459, 462, 465, 466, 468, 469, 472, 473, 474, 477, 479, 480, 483, 484, 486, 487, 518, 522, 528, 529, 530, 531, 532, 533, 537, 539, 544, 547, 549, 555, 557, 563, 565, 567, 568, 571, 572, 573, 576, 578, 581, 584, 586, 587, 592, 595, 598, 604, 629, 633, 634, 637, 639, 640, 665, 669, 671, 672, 678, 682, 683, 685, 687, 694, 701, 704, 705, 707, 710, 716, 717, 719, 723, 724, 726, 728, 730, 732, 733, 736, 737, 738, 739, 740, 743, 744, 745, 747, 750, 751, 753, 758, 760, 761, 764, 765, 766, 769, 772, 773, 775, 779, 781, 782, 785, 786, 788, 790, 791, 792, 794, 796, 797, 798, 800, 801, 806, 819, 822, 824, 825, 829, 846, 847, 850, 854, 857, 859, 861, 862, 865, 866, 868, 869, 872, 875, 877, 879, 880, 881, 884, 892, 894, 895, 896, 897, 898, 901, 902, 904, 906, 909, 910, 912, 915, 928, 930, 931, 933, 937, 940, 944, 945, 948, 950, 951, 954, 955, 959, 960, 962, 963, 964, 967, 971, 972, 977, 978, 981, 988, 989, 990, 994, 1001, 1002, 1003, 1011, 1014, 1025, 1028, 1029, 1030, 1031, 1032, 1035, 1036, 1037, 1039, 1040, 1042, 1044, 1048, 1057, 1058, 1068, 1076, 1078, 1079, 1080, 1084, 1088, 1091, 1092, 1094, 1096, 1098, 1102, 1103, 1105, 1107, 1110, 1113, 1114, 1115, 1116, 1122, 1124, 1130, 1133, 1134, 1135, 1136, 1139, 1141, 1144, 1145, 1146, 1147, 1148, 1149, 1150, 1152, 1153, 1155, 1156, 1157, 1162, 1164, 1166, 1168, 1169, 1171, 1175, 1179, 1185, 1187, 1189, 1192, 1195, 1198, 1201, 1205, 1208, 1209, 1220, 1225, 1227, 1229, 1231, 1232, 1235, 1238, 1239, 1241, 1242, 1243, 1247, 1251, 1252, 1254, 1255, 1256, 1257, 1265, 1267, 1270, 1275, 1281, 1302, 1312, 1317, 1322, 1323, 1331, 1334, 1338, 1339, 1340, 1344, 1346, 1350, 1351, 1352, 1356, 1357, 1359, 1361, 1362, 1368, 1371, 1375, 1376, 1378, 1380, 1381, 1387, 1389, 1390, 1392, 1394, 1398, 1399, 1401, 1403, 1404, 1405, 1409, 1410, 1411, 1422, 1426, 1427, 1432, 1435, 1457, 1460, 1464, 1471, 1472, 1473, 1476, 1479, 1480, 1481, 1482, 1483, 1484, 1486, 1490, 1493, 1498, 1500, 1501, 1505, 1506, 1507, 1509, 1514, 1515, 1521, 1524, 1525, 1526, 1528, 1529, 1531, 1533]

    ids = []
    with open(file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            sql, db = line.split('\t')
            # if ("IN ('") in line:
            if (" JOIN ") not in line and (" IN ") not in line and i in test_ids:
                ids.append(i)
    
    print(ids)
    print(len(ids))


"""
分析文件中每个index下的数据统计
"""
from collections import Counter
import json

def parse_line(line):
    """
    解析每一行数据
    格式: sample_id [list1] [[list2]]
    返回: (sample_id, list1, list2)
    """
    parts = line.strip().split()
    if len(parts) < 3:
        return None
    
    sample_id = int(parts[0])
    
    # 找到第一个列表
    list1_start = line.find('[')
    list1_end = line.find(']', list1_start) + 1
    list1_str = line[list1_start:list1_end]
    list1 = eval(list1_str)
    
    # 找到第二个列表（嵌套的）
    list2_start = line.find('[', list1_end)
    list2_str = line[list2_start:]
    list2 = eval(list2_str)
    
    return sample_id, list1, list2

def analyze_index_data(file_path, target_index=1):
    """
    分析指定index的数据
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # 找到目标index的数据
    current_index = None
    index_data = []
    
    for line in lines:
        if line.strip().startswith('now index:'):
            current_index = int(line.split(':')[1].strip().split('=')[0].strip())
            continue
        
        if current_index == target_index and line.strip() and not line.strip().startswith('='):
            parsed = parse_line(line)
            if parsed:
                index_data.append(parsed)
    
    return index_data

def count_list1_patterns(index_data):
    """
    统计第二个位置的列表（list1，预测正确的sql的下标）的出现模式
    """
    # 将list1转换为可哈希的格式用于计数
    list1_patterns = []
    for sample_id, list1, list2 in index_data:
        # 将列表转换为元组，使其可哈希
        pattern = tuple(list1)
        list1_patterns.append(pattern)
    
    # 统计出现次数
    counter = Counter(list1_patterns)
    
    return counter

def print_statistics(counter, total_lines):
    """
    打印统计结果
    """
    print(f"\n{'='*60}")
    print(f"总行数: {total_lines}")
    print(f"不同的list1模式数量: {len(counter)}")
    print(f"{'='*60}\n")
    
    print(f"{'预测正确的下标列表':<30} {'出现次数':<10} {'百分比':<10}")
    print(f"{'-'*60}")
    
    # 按出现次数降序排列
    for pattern, count in counter.most_common():
        # 将元组转回列表格式用于显示
        pattern_str = str(list(pattern))
        percentage = (count / total_lines) * 100
        print(f"{pattern_str:<40} {count:<10} {percentage:>6.2f}%")

def main():
    # 请根据实际情况修改文件路径
    file_path = "/media/hnu/hnu2024/wangqin/python_work/ArcticTraining/projects/arctic_text2sql_r1/temp_all.txt"
    
    print("\n分析 index == 1 的数据...")
    print("="*60)
    
    # 分析index==1的数据
    index_data = analyze_index_data(file_path, target_index=1)
    
    print(f"找到 {len(index_data)} 行数据\n")
    
    # 统计list1的模式（预测正确的sql下标）
    counter = count_list1_patterns(index_data)
    
    # 打印统计结果
    print_statistics(counter, len(index_data))
    
   
if __name__ == "__main__":
    main()

# read_sql_file()

