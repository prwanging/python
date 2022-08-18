a = 1
while a < 10:
    print(a)
    a+=1

#打印出1-30内能被3整除的数字
a = 3
while a < 30:
    print(a)
    a=a+3

#打印出1-30内，既能被3整除，又能被5整除的数
a = 1
while a <=30 :
    if a%3==0 and a%5==0:
        print(a)
    a+=1