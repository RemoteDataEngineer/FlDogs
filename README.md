# FlDogs
<h1> <b>Purpose</b>  </h1>

Showcase data in a Power Bi, sourced from a Delta Lake populated by Databricks notebooks, featuring custom DAX tables for top values, visuals with drill down and slicing functionality, bookmarks for navigation, a glossary and accompanying documentation.  

Typically, custom DAX tables are avoided as much as possible by performing all transformations in a pipeline / SSIS package / miscellaneous data transformation but it was necessary to create the LongestWaiting Dax Table as some functions aren’t allowed on fields that come from Direct Query data.  

<h1> <b> Data Architecture </b> </h1>

The data used to Upsert the Delta Lake is the free Petfinder API (Application Programming Interface). Data Architecture featuring Logic Apps, Data Factory, Databricks and a Delta Lake are described in this diagram. 

![API Petfinder Fl Dogs Data Architecture](https://github.com/RemoteDataEngineer/FlDogs/assets/140629527/0afa4e81-10e1-4149-bd2c-7c9b60e529fc)


<b>Data Architecture – Logic App</b> 

Every Sunday, a weekly Recurrence triggers a logic app that makes Restful API calls to Petfinder database and brings back thousands of dogs ready for adoption in Florida back in .json files.  

![Logic App Process Flow Diagram](https://github.com/RemoteDataEngineer/FlDogs/assets/140629527/afe4ca7c-1bf5-4b0a-ade0-e9d59e9e2b35)

<b>Data Architecture - Bulk Copy Pipeline Triggered By Logic App </b>
The Logic App then triggers a Bulk Copy Pipeline in Azure Data Factory that reads in Metadata from a .json file in blob storage and uses the Metadata to execute a Databricks Notebook for Raw to Stage and Stage to Presentation transformations.  

![Azure Data Factory Process Flow Diagram](https://github.com/RemoteDataEngineer/FlDogs/assets/140629527/00c3e6ff-42bb-4627-8083-6a24c9c9e0fd)


<b>Databricks – Raw to Stage and Stage to Presentation </b>

The Raw to Stage notebook called from ADF kicks off a series of other Databricks notebooks that mount containers, records timestamps of .json files in the Raw container, and cleans up Organization and Fl Dogs data so that it is one record per unique key, places data in .parquet files within the Stage folder.  

The Stage to Presentation notebooks kicks off a series of Databricks notebooks that refresh Hive tables for Reference (Organizations, Pictures, etc.) in the Presentation folder and Upserts a Delta Lake table containing Florida Dogs. 

<h1> <b> Power Bi </b> </h1>
Since this Power Bi refreshes from Hive tables, and most tables are refreshed through Direct Query, it may take a few minutes for the data to refresh and for the visuals to render.


<b> Power Bi - Tabs </b>

![FL Dogs Navigation](https://github.com/RemoteDataEngineer/FlDogs/assets/140629527/7b182619-a52a-4c37-9050-82b44e0c805a)

<ol>
<li> <b>Navigation </li> </b> – Page listing Bookmarks to all pages. A Bookmark to the Navigation page is on every Tab. </li>
<li> <b>Search For Dogs </b>– Search for Florida Dogs by Breed, Color, Behavior, Age, etc. </li>
<li> <b>Search For Dogs by Carousel</b> – Select pictures from carousels to see information about adoptable Florida dogs.  </li>
<li> <b> Glossary </b> – Definitions of fields displayed with reports along with sources and upsert / refresh frequencies.  </li>
<li> <b> Administrative Log </b>  – Display timestamps for ingestion, processing and presentation.  </li>
</ol>

<b> Power Bi - Custom Columns </b>

![Power Bi Custom Columns](https://github.com/RemoteDataEngineer/FlDogs/assets/140629527/5c546c66-69b8-4ff3-b508-6060c8969315)

<b> Power Bi - Troubleshooting </b>

![Troubleshooting](https://github.com/RemoteDataEngineer/FlDogs/assets/140629527/8b518a7a-6fe2-4de8-a19f-4118fa480874)

![Visual Has Exceeded Resources](https://github.com/RemoteDataEngineer/FlDogs/assets/140629527/e3148b45-749d-4d16-861a-54b739285012)
