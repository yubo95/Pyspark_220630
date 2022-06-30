import jieba
def context_jieba(data):
    seg = jieba.cut_for_search(data)
    l = list()
    for word in seg:
        l.append(word)
    return  l

def count_word(data):
    return (data, 1)
