A Flask API for Greendeck’s Data Science Assignment (Python)
By Junaid Khan (junaidkhangec@gmail.com)


Link to the API →  https://junaidsflaskapp.herokuapp.com/

Approach:-

1.The whole python code is there inside the app.py file. 

2.The API is designed using the OOP paradigm where the class RequestHandler containing all the methods and parameters. 

3.The dataset downloading and preprocessing part of the API is there inside a function called init_files() which is declared with @app.before_first_request decorator which runs only once before the first request and makes preprocessed data available for the further requests.
Function request_handler() is there to handle POST and GET requests.

4.As a part of preprocessing, using function flatten_json(), the deeply nested JSON format is converted into a flattened version in which a child key is appended with its parent and a parent is appended with its parent and so on, so that each value can be accessed directly without recursively diving deep inside a JSON tree which prevents the problem of recursion stack overflow. Also, the unnecessary data inside the dataset is filtered out using function remove_unwanted() to process the query faster.

5.The request queries are first verified for their consistency with the API, the function verify_query() is there to verify the query, the API will process the data only if the query is consistent with the API.  if the request query is not consistent then a JSON error message will be generated as response requesting to verify the query_type & filters.

6.When a query is found to be in compliance with the API, then the query will be processed by the function process_query() on the basis of query_type and then the filters will be applied, and after applying all the filters a JSON response will be generated and sent back as a response.

7.A docstring is there for each function explaining it’s functionality.

8.Supported query_type are:-
  >discounted_products_list<br> 
  >discounted_products_count<br> 
  >avg_discount<br> 
  >discounted_products_count|avg_discount<br> 
  >expensive_list<br> 
  >competition_discount_diff_list (here filters discount_diff & competition are mendatory)<br>

9.Supported filters are (considering operand1 of the filter specifying filter type with format explained in the assignment instructions):-
  >discount<br>
  >brand.name<br>
  >discount_diff<br>
  >competition<br>

10.For the query_type competition_discount_diff_list the filters discount_diff & competition are mandatory, other than that any filter can be applied to any query or a query can also be fired with no filters.

11.Testing:-
  >For the purpose of testing, Postman is used.

12.As the API is hosted on Heroku free-tier, the API goes to sleep after 30 minutes of inactivity, and when the API has slept, the first request (which wakes it up) will take more than the usual time (approx 10-20 seconds).


Thank you
