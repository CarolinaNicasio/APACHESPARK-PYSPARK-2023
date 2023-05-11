
![MasterHead](https://github.com/CarolinaNicasio/APACHESPARK-PYSPARK-2023/blob/main/images-pyspark/portadapyspark.png)
 <p align="center">
    <img src="https://img.shields.io/badge/apache-%23D42029.svg?style=for-the-badge&logo=apache&logoColor=white"/>
    </a>
    <img src="https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54"/>
    </a>
      <img src="https://github.com/arifszn/php-blog-client/actions/workflows/ci.yml/badge.svg"/>
    </a>
      <img src="https://api.codeclimate.com/v1/badges/9be4aef1d9fb784d3999/maintainability"/>
    </a>
      <img src="https://api.codeclimate.com/v1/badges/9be4aef1d9fb784d3999/test_coverage"/>
    </a>
     <img src="https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat"/>
    </a>
    <a href="https://twitter.com/Cfnicasio">
      <img src="https://img.shields.io/twitter/url?url=https%3A%2F%2Fgithub.com%2Farifszn%2Fphp-blog-client"/>
    </a>
  </p>

## TABLA DE CONTENIDO :scroll:

- [APACHE SPARK](#Apache-Spark)

- [Características](#Características)

- [Componentes](#Componentes)

- [Beneficios](#Beneficios)

- [¿Cómo usan las empresas Spark?](#¿Cómo-usan-las-empresas-Spark?)

- [¿Por qué elegir Spark en lugar de un motor solo de SQL?](#¿Por-qué-elegir-Spark-en-lugar-de-un-motor-solo-de-SQL?)

- [PYSPARK](#PYSPARK)

    - [Arquitectura de PySpark](#Arquitectura-de-PySpark)

    - [Tipos de Cluster Manager](#Tipos-de-Cluster-Manager)

    - [¿Qué es RDD (Resilient Distributed Datasets)?](#¿Qué-es-RDD-(Resilient-Distributed-Datasets)?)

    - [Crear RDD](#Crear-RDD)

    - [Funciones lambda](#Funciones-lambda)

    - [Transformaciones](#Transformaciones)

    - [Acciones](#Acciones)

    - [RDD vs Dataframe](#RDD-vs-Dataframe)
  


# APACHE SPARK

Apache Spark es un framework de computación (entorno de trabajo) en clúster open-source. Fue desarrollada originariamente en la Universidad de California, en el AMPLab de Berkeley. El código base del proyecto Spark fue donado más tarde a la Apache Software Foundation que se encarga de su mantenimiento desde entonces. Spark proporciona una interfaz para la programación de clusters completos con Paralelismo de Datos implícito y tolerancia a fallos.
Se puede considerar un sistema de computación en clúster de propósito general y orientado a la velocidad. Proporciona APIs en Java, Scala, Python y R. También proporciona un motor optimizado que soporta la ejecución de gráficos en general. También soporta un conjunto extenso y rico de herramientas de alto nivel entre las que se incluyen Spark SQL (para el procesamiento de datos estructurados basada en SQL), MLlib para implementar machine learning, GraphX para el procesamiento de graficos y Spark Streaming.

##Características

## Características

 -  Permite el procesamiento en tiempo real , con un módulo llamado Spark Streaming, que combinado con Spark SQL nos va a permitir el procesamiento en tiempo real de los datos. Conforme vayamos inyectando los datos podemos ir transformándolos y volcándolos a un resultado final.
-  Usa la evaluación perezosa, lo que significa es que todas las transformaciones que vamos realizando sobre los RDD, no se resuelven, si no que se van almacenando en un grafo acíclico dirigido (DAG) , y cuando ejecutamos una acción, es decir, cuando la herramienta no tenga más opción que ejecutar todas las transformaciones, será cuando se ejecuten. Esto es un arma de doble filo, ya que tiene una ventaja y un inconveniente. La ventaja es que se gana velocidad al no ir realizando las transformaciones continuamente, sino solo cuando es necesario. El inconveniente es que si alguna transformación eleva algún tipo de excepción, la misma no se va a detectar hasta que no se ejecute la acción, por lo que es más difícil de debuggear o programar.
- Permite trabajar en disco. De esta manera si por ejemplo tenemos un fichero muy grande o una cantidad de información que no cabe en memoria, la herramienta permite almacenar parte en disco , lo que hace perder velocidad. Esto hace que tengamos que intentar encontrar el equilibrio entre lo que se almacena en memoria y lo que se almacena en disco, para tener una buena velocidad y para que el coste no sea demasiado elevado, ya que la memoria siempre es bastante más cara que el disco.
- Trabaja en memoria, con lo que se consigue mucha mayor velocidad de procesamiento.

## Componentes
![](https://github.com/CarolinaNicasio/APACHESPARK-PYSPARK-2023/blob/main/images-pyspark/componentsapache.png)

 - Spark Streaming : Es el que permite la ingesta de datos en tiempo real. Si tenemos una fuente, por ejemplo Kafka o Twitter, con este módulo podemos ingestar los datos de esa fuente y volcarlos a un destino. Entre la ingesta de datos y su volcado posterior, podemos tener una serie de transformaciones.
- Spark SQ L: Es el módulo para el procesamiento de datos estructurados y semi-estructurados. Con este módulo vamos a poder transformar y realizar operaciones sobre los RDD o los dataframes. Está pensado exclusivamente para el tratamiento de los datos.
- Spark MLLib : Es una librería muy completa que contiene numerosos algoritmos de Machine Learning, tanto de clusterización, clasificación, regresión, etc. Nos permite, de una forma amigable, poder utilizar algoritmos de Machine Learning.
- Spark Graph : Permite el procesamiento de grafos (DAG). No permite pintar grafos, sino que permite crear operaciones con grafos, con sus nodos y aristas, e ir realizando operaciones.
- Spark Core : Es la base o conjunto de librerías donde se apoya el resto de módulos. Es el núcleo del framework.
## Beneficios
* Velocidad
Puedes ejecutar cargas de trabajo con una rapidez 100 veces mayor a la de MapReduce de Hadoop. Spark logra un alto rendimiento para datos por lotes y de transmisión mediante un programador de grafos acíclicos dirigidos de vanguardia, un optimizador de consultas y un motor de ejecución físico.
* Facilidad de uso
Spark ofrece más de 80 operadores de alto nivel que facilitan la compilación de apps paralelas. Puedes usarlo de forma interactiva desde shells de Scala, Python, R y SQL para escribir aplicaciones con rapidez.
* Generalidad
Spark suministra una pila de bibliotecas, incluidas SQL y DataFrames, MLlib para aprendizaje automático, GraphX y Spark Streaming. Puedes combinar estas bibliotecas sin problemas en la misma aplicación.
* Innovación en framework de código abierto
Spark está respaldado por comunidades globales unidas con el objetivo de presentar conceptos y funciones nuevas con mayor rapidez y de manera más efectiva que los equipos internos que desarrollan soluciones propias. El poder colectivo de una comunidad de código abierto proporciona más ideas, un desarrollo más rápido y solución de problemas cuando se presenta uno, lo que se traduce en un menor tiempo de salida al mercado. 
## ¿Cómo usan las empresas Spark?
Muchas empresas usan Spark para simplificar la tarea desafiante y de procesamiento intensivo de procesar y analizar grandes volúmenes de datos en tiempo real o archivados, así sean estructurados o no estructurados. Spark también habilita a los usuarios a integrar sin problemas funciones complejas y relevantes, como el aprendizaje automático y los algoritmos de grafos.

 | Ingenieros de datos    | Científicos de datos |
 |---------------------   |----------------------|
 |Los ingenieros de datos usan Spark para codificar y compilar trabajos de procesamiento de datos, con la opción de programar en un conjunto de lenguajes expandido.|  Los científicos de datos pueden tener una experiencia más avanzada con las estadísticas y el AA si usan Spark con GPU. La capacidad de procesar grandes volúmenes de datos con mayor rapidez en un lenguaje conocido puede ayudar a acelerar la innovación.| 


## ¿Por qué elegir Spark en lugar de un motor solo de SQL?
Apache Spark es un motor de procesamiento de clústeres rápido de uso general que puede implementarse en un clúster de Hadoop o en modo independiente. Con los programadores de Spark, se pueden escribir aplicaciones con rapidez en Java, Scala, Python, R y SQL. Esto hace que sea más accesible para los desarrolladores, los científicos de datos y los empresarios avanzados con experiencia en estadísticas. Mediante Spark SQL, los usuarios pueden conectarse a cualquier fuente de datos y presentarlas como tablas para que los clientes de SQL las usen. Además, los algoritmos interactivos de aprendizaje automático se implementan con facilidad en Spark.

Con un motor solo de SQL, como Apache Impala, Apache Hive o Apache Drill, los usuarios pueden usar solo SQL o lenguajes similares a SQL para consultar los datos almacenados en varias bases de datos. Esto significa que los frameworks tienen un menor tamaño en comparación con Spark.
## PYSPARK

 PySpark proporciona una API de Python para interactuar con Spark, lo que permite a los desarrolladores y analistas trabajar con Spark utilizando su lenguaje de programación preferido.

##  Arquitectura de PySpark

Apache Spark trabaja en una arquitectura maestro-esclavo, en la que el maestro es llamado «Driver» y los esclavos son denominados «Workers».

Cuando ejecutamos una aplicación Spark el Driver crea un contexto, el SparkContext, que es un punto de entrada a la aplicación. Todas las operaciones que se realizan (transformaciones y acciones) son ejecutadas en los Workers, y los recursos son administrados por el Cluster Manager.

![](https://github.com/CarolinaNicasio/APACHESPARK-PYSPARK-2023/blob/main/images-pyspark/Arq_py.png)


- Driver: proceso principal que ejecuta la función main() de la aplicación. Es el que crea el SparkContext.
- Worker: son todos aquellos nodos que dependen del backend y que se encargan de ejecutar los procesos de los Executors.
- Executor: es un proceso iniciado para una aplicación en un nodo Worker. Este proceso ejecuta tareas y mantiene información en memoria (o en disco). Cada aplicación tiene sus propios Executors.
- SparkContext: es el punto de entrada principal a la aplicación de Spark. Se puede usar para crear RDDs, acumuladores y variables de transmisión en el clúster.
- Cluster Manager: el administrador de clúster es un servicio que permite la comunicación del Driver con el backend para adquirir y gestionar los recursos físicos en el clúster. Es el que determina el flujo de información entre los Workers y el SparkContext.

## Tipos de Cluster Manager

 Actualmente, Spark es compatible con los administradores de clúster siguientes:

- Stand-alone (independiente): es un administrador de clúster simple que se incluye con la distribución de Spark y facilita la configuración de un clúster.
- YARN: es el gestor de recursos en Hadoop V2, que también se incluye con la distribución de Spark, y es uno de los administradores de clúster más usados. YARN funciona como un framework de computación distribuida. Aquí, tanto maestros como esclavos están altamente disponibles para su uso.
- Mesos: es otro gestor de recursos que también permite ejecutar aplicaciones PySpark y Hadoop MapReduce (ya veremos esto más adelante, por ahora quedaros con que es importante).
- Kubernetes: es un sistema de código abierto que se utiliza para automatizar la implementación, el escalado y la gestión de aplicaciones autocontenidas.
- Local: esto en realidad no es un gestor de recursos pero había que mencionarlo, ya que podemos utilizar la opción «local» al declarar el nodo máster para indicar a Spark que será tu propio ordenador.
## ¿Qué es RDD (Resilient Distributed Datasets)?

![](https://github.com/CarolinaNicasio/APACHESPARK-PYSPARK-2023/blob/main/images-pyspark/rdd_actions_transfs.png)

RDD (Resilient Distributed Datasets) es uno de los componentes que se encuentran en el Spark Core, el corazón del sistema de computación de Apache Spark. Este es uno de los servicios más reconocidos que ofrece, ya que es ideal para optimizar la gestión de los macrodatos y analizar los resultados de la información.
Estos poseen unas características específicas que ayudan a procesar los datos de una forma más eficaz. A continuación, te exponemos cuáles son estas características:
* Inmutables
 No se pueden modificar una vez han sido creados.En lugar de ello, se crean nuevos RDD a partir de los existentes mediante la aplicación de transformaciones.
* Distribuidos
Se distribuyen a través de múltiples nodos en un clúster. Esto permite que las operaciones que se realizan en los RDD puedan ejecutarse de forma paralela en diferentes nodos, lo que acelera el procesamiento de datos.
* Resilientes
Pueden ser reconstruidos en caso de fallas en alguno de los nodos que los componen. Esto se logra gracias a que los RDD se almacenan en forma de particiones en múltiples nodos del clúster.

Los RDD pueden ser creados al cargar datos de manera distribuida, como es desde un HDFS, Cassanda, Hbase o cualquier sistema de datos soportado por Hadoop, pero también por colecciones de datos de Scala o Python, además de poder ser leídos desde archivos en el sistema local.

## Crear RDD

Hay dos formas de crear RDD: paralelizar una colección existente en su programa de controlador o hacer referencia a un conjunto de datos en un sistema de almacenamiento externo, como un sistema de archivos compartido, HDFS, HBase o cualquier fuente de datos que ofrezca un formato de entrada de Hadoop.

**Colecciones paralelas**

Las colecciones paralelas se crean llamando SparkContextal parallelizemétodo en un iterable o colección existente en su programa controlador. Los elementos de la colección se copian para formar un conjunto de datos distribuido que se puede operar en paralelo. Por ejemplo, aquí se explica cómo crear una colección paralela que contenga los números del 1 al 5:

```bash
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
```
Una vez creado, el conjunto de datos distribuido ( distData) se puede operar en paralelo. Por ejemplo, podemos llamar distData.reduce(lambda a, b: a + b)para sumar los elementos de la lista. Describimos las operaciones en conjuntos de datos distribuidos más adelante.

Un parámetro importante para las colecciones paralelas es la cantidad de particiones en las que se cortará el conjunto de datos. Spark ejecutará una tarea para cada partición del clúster. Por lo general, desea de 2 a 4 particiones para cada CPU en su clúster. Normalmente, Spark intenta establecer la cantidad de particiones automáticamente en función de su clúster. Sin embargo, también puede configurarlo manualmente pasándolo como segundo parámetro a parallelize(por ejemplo, sc.parallelize(data, 10)). Nota: algunos lugares en el código usan el término segmentos (un sinónimo de particiones) para mantener la compatibilidad con versiones anteriores.

**Conjuntos de datos externos**

PySpark puede crear conjuntos de datos distribuidos desde cualquier fuente de almacenamiento compatible con Hadoop, incluido su sistema de archivos local, HDFS, Cassandra, HBase, Amazon S3 , etc. Spark admite archivos de texto, SequenceFiles y cualquier otro formato de entrada de Hadoop .

Los RDD de archivos de texto se pueden crear utilizando el método SparkContextde textFile. Este método toma un URI para el archivo (ya sea una ruta local en la máquina o un URI hdfs://, s3a://etc.) y lo lee como una colección de líneas. Aquí hay un ejemplo de invocación:


```bash
>>> distFile = sc.textFile("data.txt")
```

Una vez creado, distFilelas operaciones del conjunto de datos pueden actuar sobre él. Por ejemplo, podemos sumar los tamaños de todas las líneas usando las operaciones mapy reducede la siguiente manera: distFile.map(lambda s: len(s)).reduce(lambda a, b: a + b).

Algunas notas sobre la lectura de archivos con Spark:

Si usa una ruta en el sistema de archivos local, el archivo también debe ser accesible en la misma ruta en los nodos trabajadores. Copie el archivo a todos los trabajadores o utilice un sistema de archivos compartidos montado en la red.

Todos los métodos de entrada basados ​​en archivos de Spark, incluidos textFile, también admiten la ejecución en directorios, archivos comprimidos y comodines. Por ejemplo, puede utilizar textFile("/my/directory"), textFile("/my/directory/*.txt")y textFile("/my/directory/*.gz").

El textFilemétodo también toma un segundo argumento opcional para controlar el número de particiones del archivo. De forma predeterminada, Spark crea una partición para cada bloque del archivo (los bloques son de 128 MB de forma predeterminada en HDFS), pero también puede solicitar una mayor cantidad de particiones al pasar un valor mayor. Tenga en cuenta que no puede tener menos particiones que bloques.

Además de los archivos de texto, la API de Python de Spark también es compatible con otros formatos de datos:

SparkContext.wholeTextFilesle permite leer un directorio que contiene varios archivos de texto pequeños y devuelve cada uno de ellos como pares (nombre de archivo, contenido). Esto contrasta con textFile, que devolvería un registro por línea en cada archivo.

RDD.saveAsPickleFiley SparkContext.pickleFileadmite guardar un RDD en un formato simple que consta de objetos de Python en escabeche. El procesamiento por lotes se usa en la serialización de pickle, con un tamaño de lote predeterminado de 10.

Formatos de entrada/salida de SequenceFile y Hadoop
## Funciones lambda
En Spark, las funciones lambda se utilizan principalmente para definir transformaciones y acciones en los RDD (Resilient Distributed Datasets).Son funciones anónimas que solo pueden contener una expresión.
Las funciones lambda se originaron debido al trabajo de Alonzo Church en su cálculo lambda en el que todas las funciones eran anónimas─ en 1936, antes de la invención de las computadoras electrónicas.
La sintaxis de una función lambda es lambda args: expresión. Primero escribes la palabra clave lambda, dejas un espacio, después los argumentos que necesites separados por coma, dos puntos :, y por último la expresión que será el cuerpo de la función.
Recuerda que no puedes darle un nombre a una función lambda,  ya que estas son anónimas (sin nombre) por definición.

Una función lambda puede tener tantos argumento como necesites, pero debe tener una sola expresión.

## Transformaciones

![](https://github.com/CarolinaNicasio/APACHESPARK-PYSPARK-2023/blob/main/images-pyspark/rdd_d.png)

Las transformaciones no modifican el RDD original, sino que crean un nuevo RDD a partir de éste. Las transformaciones son perezosas (lazy), lo que significa que no se ejecutan de inmediato, sino que se aplazan hasta que se necesita el resultado final.

Las transformaciones más comunes en un RDD son:
- flatMap
- mapPartitions
- mapPartitionsWithIndex
- sample
- union
- intersection
- distinct
- groupByKey


**flatMap**

La transformación flatMap en un RDD (Resilient Distributed Dataset) de Apache Spark es similar a la transformación map, ya que también se aplica una función a cada elemento del RDD para crear un nuevo RDD con los resultados. Sin embargo, a diferencia de map, la función de flatMap devuelve una secuencia de cero o más elementos en lugar de un solo elemento.
 

Ejemplo: 
```bash
  from pyspark import SparkContext

# Crear el contexto de Spark
sc = SparkContext("local", "Ejemplo flatMap")

# Crear un RDD con una lista de cadenas de texto
rdd = sc.parallelize(["Hola mundo", "Adiós mundo"])

# Aplicar flatMap a cada línea de texto para dividir en palabras individuales
palabras_rdd = rdd.flatMap(lambda linea: linea.split(" "))

# Imprimir el resultado final
print(palabras_rdd.collect())

```
En este ejemplo, creamos un RDD llamado rddcon una lista de cadenas de texto. Luego, aplicamos la función flatMapcon una función lambda que divide cada línea en palabras individuales utilizando el separador de espacio " ". El resultado final es un nuevo RDD llamado palabras_rddque contiene todas las palabras de todas las líneas de texto. Finalmente, imprimimos el resultado utilizando la función collect()para obtener una lista de elementos del RDD. El resultado impreso será:
```bash
['Hola', 'mundo', 'Adiós', 'mundo']

```
Cada palabra individual se convierte en un elemento separado en el nuevo RDD generado por la función flatMap.

**mapPartitions**

Es una función lambda para calcular la suma de todos los elementos en cada partición del RDD:
```bash
from pyspark import SparkContext

# Crear el contexto de Spark
sc = SparkContext("local", "Ejemplo mapPartitions")

# Crear un RDD con una lista de números
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3) # 3 particiones

# Aplicar mapPartitions a cada partición para calcular la suma de todos los elementos
suma_rdd = rdd.mapPartitions(lambda particion: [sum(particion)])

# Imprimir el resultado final
print(suma_rdd.collect())
```
En este ejemplo, creamos un RDD llamado rddcon una lista de números y lo dividimos en 3 particiones utilizando el segundo argumento de la función parallelize(). Luego, aplicamos la función mapPartitionscon una función lambda que calcula la suma de todos los elementos en cada partición. La función lambda recibe como argumento un iterador que contiene todos los elementos de una partición, y devuelve una lista con un solo elemento que contiene la suma de esos elementos.

El resultado final es un nuevo RDD llamado suma_rddque contiene la suma de todos los elementos en cada partición del RDD original. Finalmente, imprimimos el resultado utilizando la función collect()para obtener una lista de elementos del RDD. El resultado impreso será:

```bash
[6, 15, 24]
```
La primera partición contiene los elementos 1, 2 y 3, cuya suma es 6. La segunda partición contiene los elementos 4, 5 y 6, cuya suma es 15. Y la tercera partición contiene los elementos 7, 8, 9 y 10, cuya suma es 24.

**mapPartitionsWithIndex**

Es una función lambda para calcular la suma de todos los elementos en cada partición del RDD, incluyendo el índice de cada partición:

```bash
from pyspark import SparkContext

# Crear el contexto de Spark
sc = SparkContext("local", "Ejemplo mapPartitionsWithIndex")

# Crear un RDD con una lista de números
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3) # 3 particiones

# Aplicar mapPartitionsWithIndex a cada partición para calcular la suma de todos los elementos
suma_rdd = rdd.mapPartitionsWithIndex(lambda indice, particion: [(indice, sum(particion))])

# Imprimir el resultado final
print(suma_rdd.collect())
```
En este ejemplo, creamos un RDD llamado rddcon una lista de números y lo dividimos en 3 particiones utilizando el segundo argumento de la función parallelize(). Luego, aplicamos la función mapPartitionsWithIndexcon una función lambda que calcula la suma de todos los elementos en cada partición y también incluye el índice de cada partición en el resultado. La función lambda recibe dos argumentos: el índice de la partición y un iterador que contiene todos los elementos de esa partición. La función devuelve una lista con un solo elemento que contiene una tupla con el índice de la partición y la suma de los elementos en esa partición.

El resultado final es un nuevo RDD llamado suma_rddque contiene una tupla para cada partición, donde el primer elemento de la tupla es el índice de la partición y el segundo elemento es la suma de todos los elementos en esa partición del RDD original. Finalmente, imprimimos el resultado utilizando la función collect()para obtener una lista de elementos del RDD. El resultado impreso será:
```bash
[(0, 6), (1, 15), (2, 24)]
```
La primera tupla contiene el índice 0 y la suma de los elementos 1, 2 y 3 en la primera división. La segunda tupla contiene el índice 1 y la suma de los elementos 4, 5 y 6 en la segunda división. Y la tercera tupla contiene el índice 2 y la suma de los elementos 7, 8, 9 y 10 en la tercera división.
**groupByKey**

La función groupByKeyen PySpark se utiliza para agrupar los valores de un RDD por clave.

```bash
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("groupByKeyExample")
sc = SparkContext.getOrCreate(conf)

# Crea un RDD con pares (clave, valor)
data = [(1, 2), (1, 4), (2, 3), (2, 5), (2, 6)]
rdd = sc.parallelize(data)

# Agrupa los valores por clave utilizando groupByKey
grouped = rdd.groupByKey()

# Muestra los resultados
result = grouped.map(lambda x: (x[0], list(x[1]))).collect()
print(result)

```
En este ejemplo, primero creamos un RDD con pares (clave, valor) utilizando la función parallelize. Luego, utilizamos la función groupByKeypara agrupar los valores por clave. Finalmente, utilizamos una función lambda para convertir los valores agrupados en una lista y recopilar los resultados utilizando la función collect. El resultado de este ejemplo seria:
```bash
[(1, [2, 4]), (2, [3, 5, 6])]
```
Lo que significa que los valores de clave 1 son [2, 4] y los valores de clave 2 son [3, 5, 6].

**intersection**

Devuelve un nuevo RDD que contiene la intersección de elementos en el conjunto de datos de origen y el argumento.
```bash
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("intersectionExample")
sc = SparkContext.getOrCreate(conf)

# Crea dos RDDs con valores únicos
rdd1 = sc.parallelize([1, 2, 3, 4, 5])
rdd2 = sc.parallelize([4, 5, 6, 7, 8])

# Encuentra la intersección utilizando intersection
intersection = rdd1.intersection(rdd2)

# Muestra los resultados
result = intersection.collect()
print(result)

```
En este ejemplo, primero creamos dos RDDs con valores únicos utilizando la función parallelize. Luego, utilizamos la función intersection para encontrar los elementos comunes de los dos RDDs. Finalmente, recopilamos los resultados utilizando la función collect. El resultado de este ejemplo sería:

```bash
[4, 5]

```
Lo que significa que los valores comunes de los RDDs rdd1 y rdd2 son 4 y 5.

**Otras Transformaciones** 

| Transformación  | Función |
| ------------- |:-------------:|
| sample(withReplacement, fraction, seed)     | Muestra una fracción de los datos, con o sin reemplazo, utilizando una semilla generadora de números aleatorios dada.     |
| union(otherDataset)      | 	Devuelve un nuevo conjunto de datos que contiene la unión de los elementos en el conjunto de datos de origen y el argumento.     |
| distinct([numPartitions]))	      | Devuelve un nuevo conjunto de datos que contiene los distintos elementos del conjunto de datos de origen.    |
|groupByKey([numPartitions])| Cuando se invoca en un conjunto de datos de pares (K, V), devuelve un conjunto de datos de pares (K, Iterable).Nota: si está agrupando para realizar una agregación (como una suma o un promedio) sobre cada clave, el uso de reduceByKeyo aggregateByKeyproducirá un rendimiento mucho mejor.Nota: De forma predeterminada, el nivel de paralelismo en la salida depende del número de particiones del RDD principal. Puede pasar un numPartitionsargumento opcional para establecer un número diferente de tareas.       |
|reduceByKey(func, [numPartitions])	| Cuando se invoca en un conjunto de datos de pares (K, V), devuelve un conjunto de datos de pares (K, V) donde los valores de cada clave se agregan utilizando la función de reducción dada func , que debe ser del tipo (V,V) = > V. Al igual que en groupByKey, el número de tareas de reducción se puede configurar a través de un segundo argumento opcional.              |
|aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions])| Cuando se invoca en un conjunto de datos de pares (K, V), devuelve un conjunto de datos de pares (K, U) donde los valores de cada clave se agregan utilizando las funciones de combinación dadas y un valor "cero" neutral. Permite un tipo de valor agregado que es diferente al tipo de valor de entrada, al tiempo que evita asignaciones innecesarias. Al igual que en groupByKey, el número de tareas de reducción se puede configurar a través de un segundo argumento opcional.     |
|sortByKey([ascending], [numPartitions])| Cuando se invoca en un conjunto de datos de pares (K, V) donde K implementa Ordenado, devuelve un conjunto de datos de pares (K, V) ordenados por claves en orden ascendente o descendente, como se especifica en el argumento booleano ascending.                 |
|join(otherDataset, [numPartitions])|     Cuando se invoca en conjuntos de datos de tipo (K, V) y (K, W), devuelve un conjunto de datos de pares (K, (V, W)) con todos los pares de elementos para cada clave. Las uniones externas se admiten mediante leftOuterJoin, rightOuterJoiny fullOuterJoin.    |
|cogroup(otherDataset, [numPartitions])	|  Cuando se invoca en conjuntos de datos de tipo (K, V) y (K, W), devuelve un conjunto de datos de (K, (Iterable<V>, Iterable<W>)) tuplas. Esta operación también se llama groupWith.                    |
|cartesian(otherDataset)| Cuando se invoca en conjuntos de datos de tipos T y U, devuelve un conjunto de datos de pares (T, U) (todos los pares de elementos).             |
|pipe(command, [envVars])| Canalice cada partición del RDD a través de un comando de shell, por ejemplo, un script Perl o bash. Los elementos RDD se escriben en la entrada estándar del proceso y las líneas de salida a su salida estándar se devuelven como un RDD de cadenas.            |
|coalesce(numPartitions)|  Disminuya el número de particiones en el RDD a numPartitions. Útil para ejecutar operaciones de manera más eficiente después de filtrar un gran conjunto de datos.                          |
|repartition(numPartitions)	|   Reorganiza los datos en el RDD aleatoriamente para crear más o menos particiones y equilibrarlos entre ellas. Esto siempre mezcla todos los datos a través de la red.                       |
|repartitionAndSortWithinPartitions(partitioner)| Vuelva a particionar el RDD de acuerdo con el particionador dado y, dentro de cada partición resultante, ordene los registros por sus claves. Esto es más eficiente que llamar repartitiony luego clasificar dentro de cada partición porque puede empujar la clasificación hacia abajo en la maquinaria aleatoria.                      | 

## Acciones

 Una vez se logre la estructura deseada de sus datos, pódremos realizar diferentes acciones. Las operaciones de acción representan estructuras que no son RDD y proporcionan un valor específico obtenido de los datos con los que está trabajando. Se utilizan con ejecutores en diferentes clústeres para que pueda realizar tareas en dos nodos diferentes de los clústeres. Los operadores de acción trabajan principalmente con ejecutores para enviar datos al controlador en la estructura del clúster.
 |Acción| Función |
| ------------- |:-------------:|
| reduce(func)	     | Agrega los elementos del conjunto de datos usando una función func (que toma dos argumentos y devuelve uno). La función debe ser conmutativa y asociativa para que pueda calcularse correctamente en paralelo.    |
| collect()	    | Devuelve todos los elementos del conjunto de datos como una matriz en el programa controlador. Esto suele ser útil después de un filtro u otra operación que devuelve un subconjunto suficientemente pequeño de los datos.    |
| count()	   | Devuelve el número de elementos en el conjunto de datos.    |
|first()	| 	Devuelve el primer elemento del conjunto de datos (similar a take(1)). |
|take(n)|  Devuelve una matriz con los primeros n elementos del conjunto de datos.                   |
|takeSample(withReplacement, num, [seed])| Devuelve una matriz con una muestra aleatoria de num elementos del conjunto de datos, con o sin reemplazo, opcionalmente especificando previamente una semilla generadora de números aleatorios.    |
|takeOrdered(n, [ordering])	|    Devuelve los primeros n elementos del RDD usando su orden natural o un comparador personalizado.             |
| saveAsTextFile(path)|   	Escriba los elementos del conjunto de datos como un archivo de texto (o conjunto de archivos de texto) en un directorio determinado en el sistema de archivos local, HDFS o cualquier otro sistema de archivos compatible con Hadoop. Spark llamará a toString en cada elemento para convertirlo en una línea de texto en el archivo.                       |
|  saveAsSequenceFile(path)(Java and Scala)|  Escriba los elementos del conjunto de datos como Hadoop SequenceFile en una ruta determinada en el sistema de archivos local, HDFS o cualquier otro sistema de archivos compatible con Hadoop. Está disponible en RDD de pares clave-valor que implementan la interfaz de escritura de Hadoop. En Scala, también está disponible en tipos que se pueden convertir implícitamente a Writable (Spark incluye conversiones para tipos básicos como Int, Double, String, etc.).            |
|saveAsObjectFile(path)(Java and Scala) |   Escriba los elementos del conjunto de datos en un formato simple usando la serialización de Java, que luego se puede cargar usando SparkContext.objectFile().                 |
|countByKey()|  Solo disponible en RDD de tipo (K, V). Devuelve un hashmap de pares (K, Int) con el recuento de cada clave.                     |
|foreach(func)	|   Ejecute una función en cada elemento del conjunto de datos. Esto generalmente se hace por efectos secundarios, como actualizar un acumulador o interactuar con sistemas de almacenamiento externos.Nota : la modificación de variables que no sean Acumuladores fuera del foreach()puede resultar en un comportamiento indefinido. Consulte Comprender los cierres para obtener más detalles.   |


**collect()**

La acción collect()en PySpark se utiliza para recuperar todos los elementos de un RDD (Resilient Distributed Dataset) en forma de una lista en la memoria del controlador. Esta acción puede ser útil para pequeñas cantidades de datos, pero no se recomienda para grandes conjuntos de datos, ya que puede causar problemas de memoria.
Supongamos que tenemos un RDD llamado rddque contiene una lista de números:

```bash
rdd = sc.parallelize([1, 2, 3, 4, 5])

```
Luego, podemos aplicar la acción collect()para recuperar todos los elementos del RDD en una lista en la memoria del controlador:
```bash
collected_list = rdd.collect()

```

Ahora, collected_listcontendrá [1, 2, 3, 4, 5], que son los elementos del RDD rdd.

Es importante tener en cuenta que, como se mencionó anteriormente, la acción collect()debe usarse con precaución en conjuntos de datos grandes, ya que puede causar problemas de memoria en el controlador. Además, si el RDD tiene muchos elementos, la transferencia de datos desde los nodos de trabajo al controlador puede llevar mucho tiempo. En general, es mejor utilizar las transformaciones y acciones de PySpark que se ejecutan en los nodos de trabajo para procesar grandes conjuntos de datos.

**foreach(func)**

Supongamos que tenemos un RDD llamado rdd que contiene una lista de números:
```bash
rdd = sc.parallelize([1, 2, 3, 4, 5])
```

Luego, podemos aplicar la acción foreach(func) para imprimir cada elemento del RDD:
```bash
def print_element(element):
    print(element)

rdd.foreach(print_element)

```

En este caso, print_element es una función que toma un elemento como argumento y lo imprime en la consola. Al aplicar la acción foreach(print_element) al RDD rdd, se imprimirán los elementos 1, 2, 3, 4 y 5 en la consola.

Es importante tener en cuenta que la función func que se pasa a la acción foreach debe ser serializable, ya que se ejecutará en los nodos de trabajo en paralelo. Además, la acción foreach no devuelve ningún resultado y solo realiza una operación en cada elemento del RDD. Si se desea obtener un nuevo RDD después de aplicar una función a cada elemento, se debe utilizar una transformación en lugar de una acción.


## RDD vs Dataframe

Una de las grandes ventajas que ofrecen los RDD es la compilación segura; por su particularidad de ejecución perezosa, se calcula si se generará un error o no antes de ejecutarse, lo cual permite identificar problemas antes de lanzar la aplicación. El pero que podemos encontrar con los RDD es que no son correctamente tratados por el Garbage collector y cuando las lógicas de operación se hacen complejas, su uso puede resultar poco práctico, aquí entran los DataFrames.

## DATAFRAME

El DataFrame de pyspark es la estructura más optimizada en Machine Learning. Utiliza las bases subyacentes de un RDD pero se ha estructurado en columnas además de filas en una estructura SQL. Su forma está inspirada en el DataFrame del módulo Pandas.

Gracias a la estructura de DataFrame, podemos por tanto realizar potentes cálculos a través de un lenguaje familiar (ya que es similar a pandas), a la vez que evitamos el coste de entrada de aprender un nuevo lenguaje funcional: Scala.

Spark SQL es un módulo de Spark que permite trabajar con datos estructurados. Por lo tanto, dentro de este módulo se ha desarrollado el Spark DataFrame.

Los DataFrames implementan un sistema llamado Catalyst, el cual es un motor de optimización de planes de ejecución, parecido al que usan las bases de datos, pero diseñado para la cantidad de datos propia de Spark, aunado a eso, se tiene implementado un optimizador de memoria y consumo de CPU llamado Tungsten, el cual determina cómo se convertirán los planes lógicos creados por Catalyst a un plan físico.

## Creación de Dataframe

  - Usando createDataFrame()
  Al usar createDataFrame()la función de SparkSession, puede crear un DataFrame.
  ```bash
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
```
Dado que los DataFrame tienen un formato de estructura que contiene nombres y columnas, podemos obtener el esquema del DataFrame usandodf.printSchema()

 ```bash
 
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)

 ```
 df.show()muestra los 20 elementos del DataFrame.

```bash
+---------+----------+--------+----------+------+------+
|firstname|middlename|lastname|dob       |gender|salary|
+---------+----------+--------+----------+------+------+
|James    |          |Smith   |1991-04-01|M     |3000  |
|Michael  |Rose      |        |2000-05-19|M     |4000  |
|Robert   |          |Williams|1978-09-05|M     |4000  |
|Maria    |Anne      |Jones   |1967-12-01|F     |4000  |
|Jen      |Mary      |Brown   |1980-02-17|F     |-1    |
+---------+----------+--------+----------+------+------+
 ```

