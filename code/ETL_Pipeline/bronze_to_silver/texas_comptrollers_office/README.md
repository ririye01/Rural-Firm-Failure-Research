# **Texas Comptroller's Office Data**

## **Functionality Guide**

Datasets and API docs can be found <a href="https://data.texas.gov/browse?Dataset-Category_Agency=Texas+Comptroller+of+Public+Accounts&limitTo=datasets&provenance=official">here</a>. A username and password are required to scrape large amounts of data from Socrata, and given the size of the Texas Comptroller's Office dataset, access to call the API this amount of times is essential. 

Socrata GitHub repository and installation for pulling the data can be found <a href="https://github.com/socrata/socrata-py">here</a> as well.

To ensure that you can pull the entire Active Franchise Taxholder dataset from the API, create an account and follow <a href="https://data.texas.gov/browse?Dataset-Category_Agency=Texas+Comptroller+of+Public+Accounts&limitTo=datasets&provenance=official">this link</a> to create a Socrata client. On a Mac, execute the following commands in the Terminal, and replace `<username>` and `<password>` with the corresponding username and password you used to create your Socrata account. 

```bash
echo 'export SOCRATA_USERNAME="<username>"' >> ~/.zshrc
echo 'export SOCRATA_PASSWORD="<password>"' >> ~/.zshrc
```


