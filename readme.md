PythonDeveloperAssignment
___
 Написать программу на Python, которая делает следующие действия:
1. Создает 50 zip-архивов, в каждом 100 xml файлов со случайными данными следующей структуры:
```
<root>
<varname=’id’ value=’<случайное уникальное строковое значение>’/>
<varname=’level’ value=’<случайное число от 1 до 100>’/>
<objects>
<objectname=’<случайное строковое значение>’/>
<objectname=’<случайное строковое значение>’/>
 …
</objects>
</root>
```
В тэге objects случайное число (от 1 до 10) вложенных тэгов object.

2. Обрабатывает директорию с полученными zip архивами, разбирает вложенные xml файлы и формирует 2 csv файла:
 Первый: id, level - по одной строке на каждый xml файл
 Второй: id, object_name - по отдельной строке для каждого тэга object (получится от 1 до 10 строк на каждый xml файл)
Очень желательно сделать так, чтобы задание 2 эффективно использовало ресурсы многоядерного процессора. 
**Также желательно чтобы программа работала быстро**.
___

