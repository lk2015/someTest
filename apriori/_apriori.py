def apriori(D, minSup):
    C1 = {}
    for T in D:
        for I in T:
            if I in C1:
                C1[I] += 1
            else:
                C1[I] = 1

    _keys1 = C1.keys()


    keys1 = []
    for i in _keys1:
        keys1.append([i])


    n = len(D)
    cutKeys1 = []
    for k in keys1[:]:
        if C1[k[0]] * 1.0 / n >= minSup:
            cutKeys1.append(k)

    cutKeys1.sort()

    keys = cutKeys1
    all_keys = []



    while keys != []:
        C = getC(D, keys)
        cutKeys = getCutKeys(keys, C, minSup, len(D))
        for key in cutKeys:
            all_keys.append(key)
        keys = aproiri_gen(cutKeys)

    return all_keys


def getC(D, keys):
    C = []
    for key in keys:
        c = 0
        for T in D:
            have = True
            for k in key:
                if k not in T:
                    have = False
            if have:
                c += 1
        C.append(c)
    return C


def getCutKeys(keys, C, minSup, length):
    for i, key in enumerate(keys):
        if float(C[i]) / length < minSup:
            keys.remove(key)
    return keys




def aproiri_gen(keys1):
    keys2 = []
    for k1 in keys1:
        for k2 in keys1:
            if k1 != k2:
                key = []
                for k in k1:
                    if k not in key:
                        key.append(k)
                for k in k2:
                    if k not in key:
                        key.append(k)
                key.sort()
                if len(key)==(len(k1)+1) and key not in keys2:
                    keys2.append(key)
    return keys2


