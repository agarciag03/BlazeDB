# Task 1: Explanation about the logic used for extracting join conditions from WHERE clause.


# Task 2: Optimisation rules/ Why they are correct / how they reduce the size of intermediate results during query evaluation.


Aplicar selección y proyección varias veces no es un problema en sí mismo, siempre y cuando lo hagas de manera eficiente. De hecho, hacer selecciones y proyecciones varias veces puede ser una forma de reducir la cantidad de datos que se procesan en las etapas posteriores.

Es más importante tener en cuenta los siguientes puntos:
	1.	Selecciones antes de los joins: Al aplicar la selección antes de los joins, reduces la cantidad de datos involucrados en las operaciones de join, lo que puede mejorar significativamente el rendimiento.
	2.	Proyecciones antes de los joins: Si aplicas proyecciones antes del join, solo estarás trabajando con las columnas necesarias. Esto reduce la carga de procesamiento cuando las tuplas resultantes de los joins son grandes.
	3.	Selección y proyección finales: Después de los joins y otros operadores, asegúrate de aplicar las selecciones y proyecciones finales como lo requiere el query.