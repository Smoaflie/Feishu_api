class Obj(dict):
    """自定义对象类,用于将字典转换成对象"""

    def __init__(self, d):
        for a, b in d.items():
            if isinstance(b, (list, tuple)):
                setattr(self, a, [Obj(x) if isinstance(x, dict) else x for x in b])
            else:
                setattr(self, a, Obj(b) if isinstance(b, dict) else b)


def dict_2_obj(d: dict):
    return Obj(d)


def obj_2_dict(o: Obj) -> dict:
    r = {}
    for a, b in o.__dict__.items():
        if isinstance(b, str):
            r[a] = b
        elif isinstance(b, (list, tuple)):
            r[a] = [obj_2_dict(x) if isinstance(x, Obj) else x for x in b]
        elif isinstance(b, Obj):
            r[a] = obj_2_dict(b)
    return r