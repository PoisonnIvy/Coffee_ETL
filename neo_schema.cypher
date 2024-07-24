// COUNTRY
(C:COUNTRY{name:"string"}),

// BEAN
(B:BEAN{species:"string",moisture:float}),

// GRADE
(G:GRADE{
    aroma:float,
    flavor:float,
    aftertaste:float,
    acidity:float,
    body_mouthfeel:float,
    balance:float,
    uniformity:float,
    clean_cup:float,
    sweetness:float,
    overall:float
    }),

//BEAN COMES FROM COUNTRY RELATION
(BEAN)-[:IS_FROM {harvest_year:int}] -> (COUNTRY) 

//BEAN HAS GRADE RELATION
(BEAN)-[:HAS_GRADE {grading_year:int, total_points:float}] -> (GRADE)





