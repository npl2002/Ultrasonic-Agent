class Dog:
    """一个表示狗的类"""

    def __init__(self, name: str, age: int, breed: str = "未知品种"):
        self.name = name
        self.age = age
        self.breed = breed

    def bark(self):
        """狗叫的方法"""
        print(f"{self.name}：dnmd")

    def eat(self, food: str):
        """吃东西"""
        print(f"{self.name} 正在吃 {food}。")

    def dog_action(self, action: str):
        print(f"{self.name} 开始 {action}。")

    def get_older(self):
        """年龄加一"""
        self.age += 1
        print(f"{self.name} 过了一年，现在 {self.age} 岁了。")

    def __str__(self):
        """打印狗的信息"""
        return f"名字：{self.name}, 年龄：{self.age}, 品种：{self.breed}"

# 创建一只狗
dog1 = Dog(name="景浩哲", age=23, breed="卷毛")

# 让狗叫
dog1.bark()

# 喂它吃东西
dog1.eat("大份")

dog1.dog_action("摇尾巴")

# 狗长大一岁
dog1.get_older()

# 打印狗的信息
print(dog1)