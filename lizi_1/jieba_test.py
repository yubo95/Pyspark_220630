import jieba
if __name__ == '__main__':
    content = "二次练习玉博"

    print(list(jieba.cut(content, True)))