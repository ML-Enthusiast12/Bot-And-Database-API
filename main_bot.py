from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import asyncpg
import aiomysql
import motor.motor_asyncio
import logging
from fastapi.middleware.cors import CORSMiddleware

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Bot & Database API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic Models
class BotInfo(BaseModel):
    bot_id: str
    bot_type: str
    agent_type: str
    theme: str
    description: str
    url: str
    chat_history_db: str
    vector_db: str

class DatabaseConnection(BaseModel):
    host: str
    port: int
    username: str
    password: str
    schema: str
    tableNames: List[str] = []
    dbtype: str

# Global variables to store session data
db_connections = {}
session_schemas = {}

# Custom exception handler
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail, "status_code": exc.status_code}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"error": f"Internal server error: {str(exc)}", "status_code": 500}
    )

@app.get("/getbotsinfo", response_model=List[BotInfo])
async def get_bots_info():
    base_url = "http://103.168.18.197:3000/chat"
    bots = [
        {
            "bot_id": "bot_001",
            "bot_type": "weace",
            "agent_type": "inhouse",
            "theme": "light",
            "description": "AI-assisted journey for a coaching platform featuring profile completion, goal preferences, coach recommendations, and session scheduling to help users achieve their personal and professional goals.",
            "url": f"{base_url}/weace",
            "chat_history_db": "weace_chat_db",
            "vector_db": "weace_vector_db"
        },
        {
            "bot_id": "bot_002",
            "bot_type": "lyfe",
            "agent_type": "inhouse",
            "theme": "dark",
            "description": "Medical AI document assistant that gracefully handles medical documents, allowing users to analyze insights, extract key information, and make informed decisions from their healthcare data.",
            "url": f"{base_url}/lyfe",
            "chat_history_db": "lyfe_chat_db",
            "vector_db": "lyfe_vector_db"
        },
        {
            "bot_id": "bot_003",
            "bot_type": "travelAssistant",
            "agent_type": "elevenlabs",
            "theme": "light",
            "description": "Voice-powered travel booking agent that assists users in planning trips, booking flights and hotels, and providing real-time travel recommendations through natural conversation.",
            "url": f"{base_url}/travelAssistant",
            "chat_history_db": "travel_chat_db",
            "vector_db": "travel_vector_db"
        }
    ]
    return bots

@app.post("/connectDB")
async def connect_database(connection: DatabaseConnection):
    try:
        dbtype = connection.dbtype.lower().strip()
        logger.info(f"Attempting to connect to '{dbtype}' database at {connection.host}:{connection.port}")
        
        if dbtype == "postgresql":
            return await connect_postgresql(connection)
        elif dbtype == "mysql":
            return await connect_mysql(connection)
        elif dbtype == "mongodb":
            return await connect_mongodb(connection)
        else:
            logger.error(f"Unsupported database type: '{connection.dbtype}'")
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported database type: '{connection.dbtype}'. Supported types are: postgresql, mysql, mongodb"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

async def connect_postgresql(connection: DatabaseConnection):
    conn = None
    try:
        logger.info(f"Connecting to PostgreSQL at {connection.host}:{connection.port}, database: {connection.schema}")
        
        conn = await asyncpg.connect(
            host=connection.host,
            port=connection.port,
            user=connection.username,
            password=connection.password,
            database=connection.schema
        )
        
        logger.info("Successfully connected to PostgreSQL")
        
        # Fetch all schemas
        schemas_query = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema');
        """
        schemas = await conn.fetch(schemas_query)
        schema_names = [schema['schema_name'] for schema in schemas]
        logger.info(f"Retrieved {len(schema_names)} schemas")
        
        table_info = {}
        
        for schema_name in schema_names:
            tables_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = $1 
                AND table_type = 'BASE TABLE';
            """
            tables = await conn.fetch(tables_query, schema_name)
            table_names = [table['table_name'] for table in tables]
            logger.info(f"Retrieved {len(table_names)} tables from schema {schema_name}")
            
            table_info[schema_name] = {}
            for table_name in table_names:
                query = """
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length
                    FROM information_schema.columns
                    WHERE table_schema = $1 
                    AND table_name = $2
                    ORDER BY ordinal_position;
                """
                
                columns = await conn.fetch(query, schema_name, table_name)
                
                table_info[schema_name][table_name] = [
                    {
                        "column_name": col['column_name'],
                        "data_type": col['data_type'],
                        "is_nullable": col['is_nullable'],
                        "default_value": str(col['column_default']) if col['column_default'] else None,
                        "max_length": col['character_maximum_length']
                    }
                    for col in columns
                ]
                logger.info(f"Found {len(columns)} columns for table {table_name} in schema {schema_name}")
        
        session_id = f"{connection.host}:{connection.port}:{connection.schema}"
        db_connections[session_id] = connection.dict()
        session_schemas[session_id] = table_info
        
        return {
            "status": "success",
            "session_id": session_id,
            "schema_info": table_info
        }
    
    except asyncpg.PostgresError as e:
        error_msg = f"PostgreSQL error: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)
    finally:
        if conn:
            await conn.close()
            logger.info("PostgreSQL connection closed")

async def connect_mysql(connection: DatabaseConnection):
    conn = None
    try:
        logger.info(f"Connecting to MySQL at {connection.host}:{connection.port}")
        
        conn = await aiomysql.connect(
            host=connection.host,
            port=connection.port,
            user=connection.username,
            password=connection.password,
            db=connection.schema
        )
        
        logger.info("Successfully connected to MySQL")
        
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.execute("SHOW TABLES")
            tables = await cursor.fetchall()
            table_names = [list(table.values())[0] for table in tables]
            logger.info(f"Retrieved {len(table_names)} tables from schema {connection.schema}")
        
        table_info = {}
        
        async with conn.cursor(aiomysql.DictCursor) as cursor:
            for table_name in table_names:
                logger.info(f"Fetching schema for table: {table_name}")
                
                query = """
                    SELECT 
                        COLUMN_NAME as column_name,
                        DATA_TYPE as data_type,
                        IS_NULLABLE as is_nullable,
                        COLUMN_DEFAULT as default_value,
                        CHARACTER_MAXIMUM_LENGTH as max_length
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s 
                    AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION;
                """
                
                await cursor.execute(query, (connection.schema, table_name))
                columns = await cursor.fetchall()
                
                table_info[table_name] = [
                    {
                        "column_name": col['column_name'],
                        "data_type": col['data_type'],
                        "is_nullable": col['is_nullable'],
                        "default_value": str(col['default_value']) if col['default_value'] else None,
                        "max_length": col['max_length']
                    }
                    for col in columns
                ]
                logger.info(f"Found {len(columns)} columns for table: {table_name}")
        
        session_id = f"{connection.host}:{connection.port}:{connection.schema}"
        db_connections[session_id] = connection.dict()
        session_schemas[session_id] = table_info
        
        return {
            "status": "success",
            "session_id": session_id,
            "schema_info": table_info
        }
    
    except aiomysql.Error as e:
        error_msg = f"MySQL error: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)
    finally:
        if conn:
            conn.close()
            logger.info("MySQL connection closed")

async def connect_mongodb(connection: DatabaseConnection):
    client = None
    try:
        logger.info(f"Connecting to MongoDB at {connection.host}:{connection.port}")
        
        client = motor.motor_asyncio.AsyncIOMotorClient(
            f"mongodb://{connection.username}:{connection.password}@{connection.host}:{connection.port}/"
        )
        
        db = client[connection.schema]
        
        await client.server_info()
        logger.info("Successfully connected to MongoDB")
        
        collection_names = await db.list_collection_names()
        logger.info(f"Retrieved {len(collection_names)} collections from schema {connection.schema}")
        
        table_info = {}
        
        for collection_name in collection_names:
            logger.info(f"Fetching schema for collection: {collection_name}")
            collection = db[collection_name]
            
            sample_doc = await collection.find_one()
            
            if sample_doc:
                columns = [
                    {
                        "column_name": key,
                        "data_type": type(value).__name__,
                        "is_nullable": "YES",
                        "default_value": None,
                        "max_length": None
                    }
                    for key, value in sample_doc.items()
                ]
                table_info[collection_name] = columns
                logger.info(f"Found {len(columns)} fields in collection: {collection_name}")
            else:
                logger.warning(f"Collection '{collection_name}' is empty or does not exist")
                table_info[collection_name] = []
        
        session_id = f"{connection.host}:{connection.port}:{connection.schema}"
        db_connections[session_id] = connection.dict()
        session_schemas[session_id] = table_info
        
        return {
            "status": "success",
            "session_id": session_id,
            "schema_info": table_info
        }
    
    except Exception as e:
        error_msg = f"MongoDB connection error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)
    finally:
        if client:
            client.close()
            logger.info("MongoDB connection closed")

@app.post("/setsessionSchema")
async def set_session_schema(session_id: str, schema: Dict[str, List[Dict[str, Any]]]):
    try:
        logger.info(f"Setting session schema for session: {session_id}")
        
        if session_id not in db_connections:
            logger.error(f"Session not found: {session_id}")
            raise HTTPException(status_code=404, detail="Session not found. Please connect to database first.")
        
        for table_name, columns in schema.items():
            if not isinstance(columns, list):
                raise HTTPException(status_code=400, detail=f"Invalid schema for table {table_name}. Expected a list of columns.")
            
            for idx, column in enumerate(columns):
                if not isinstance(column, dict):
                    raise HTTPException(status_code=400, detail=f"Invalid column definition at index {idx} in table {table_name}")
                if "column_name" not in column:
                    raise HTTPException(status_code=400, detail=f"Missing 'column_name' in column at index {idx} in table {table_name}")
        
        session_schemas[session_id] = schema
        logger.info(f"Successfully set schema for {len(schema)} tables in session: {session_id}")
        
        return {
            "status": "success",
            "message": "Session schema set successfully",
            "session_id": session_id,
            "tables": list(schema.keys())
        }
    
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"Failed to set session schema: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

@app.get("/sessions")
async def get_active_sessions():
    return {
        "active_sessions": list(db_connections.keys()),
        "session_details": [
            {
                "session_id": session_id,
                "dbtype": details["dbtype"],
                "host": details["host"],
                "port": details["port"],
                "schema": details["schema"],
                "has_schema_defined": session_id in session_schemas
            }
            for session_id, details in db_connections.items()
        ]
    }

@app.get("/session/{session_id}/schema")
async def get_session_schema(session_id: str):
    if session_id not in db_connections:
        raise HTTPException(status_code=404, detail="Session not found")
    
    if session_id not in session_schemas:
        raise HTTPException(status_code=404, detail="Session schema not defined")
    
    return {
        "session_id": session_id,
        "schema": session_schemas[session_id],
        "connection_info": {
            "dbtype": db_connections[session_id]["dbtype"],
            "host": db_connections[session_id]["host"],
            "schema": db_connections[session_id]["schema"]
        }
    }

@app.delete("/session/{session_id}")
async def delete_session(session_id: str):
    if session_id not in db_connections:
        raise HTTPException(status_code=404, detail="Session not found")
    
    del db_connections[session_id]
    if session_id in session_schemas:
        del session_schemas[session_id]
    
    logger.info(f"Deleted session: {session_id}")
    
    return {
        "status": "success",
        "message": f"Session {session_id} deleted successfully"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "active_sessions": len(db_connections),
        "configured_schemas": len(session_schemas),
        "supported_databases": ["postgresql", "mysql", "mongodb"]
    }

@app.get("/")
async def root():
    return {
        "message": "Welcome to Database Connections API",
        "version": "1.0.0",
        "endpoints": {
            "/getbotsinfo": "Get information about available bots",
            "/connectDB": "Connect to a database and retrieve table information",
            "/setsessionSchema": "Set schema for a database session",
            "/sessions": "Get all active database sessions",
            "/session/{session_id}/schema": "Get schema for a specific session",
            "/session/{session_id}": "Delete a database session",
            "/health": "Health check endpoint",
            "/example/connectDB": "Example request for connecting to databases",
            "/example/setsessionSchema": "Example request for setting session schema",
            "/docs": "Interactive API documentation",
            "/redoc": "Alternative API documentation"
        }
    }

@app.get("/example/connectDB")
async def example_connect_db():
    return {
        "description": "Example request for connecting to different databases",
        "postgresql_example": {
            "host": "localhost",
            "port": 5432,
            "username": "myuser",
            "password": "mypassword",
            "schema": "mydatabase",
            "tableNames": [],
            "dbtype": "postgresql"
        },
        "mysql_example": {
            "host": "localhost",
            "port": 3306,
            "username": "root",
            "password": "password",
            "schema": "mydb",
            "tableNames": [],
            "dbtype": "mysql"
        },
        "mongodb_example": {
            "host": "localhost",
            "port": 27017,
            "username": "admin",
            "password": "password",
            "schema": "mydb",
            "tableNames": [],
            "dbtype": "mongodb"
        }
    }

@app.get("/example/setsessionSchema")
async def example_set_session_schema():
    return {
        "description": "Example request for setting session schema",
        "method": "POST",
        "url": "/setsessionSchema?session_id=localhost:5432:mydatabase",
        "body": {
            "users": [
                {
                    "column_name": "id",
                    "data_type": "integer",
                    "is_nullable": "NO",
                    "is_primary_key": True
                },
                {
                    "column_name": "username",
                    "data_type": "varchar",
                    "is_nullable": "NO",
                    "max_length": 50
                },
                {
                    "column_name": "email",
                    "data_type": "varchar",
                    "is_nullable": "YES",
                    "max_length": 100
                }
            ],
            "products": [
                {
                    "column_name": "id",
                    "data_type": "integer",
                    "is_nullable": "NO",
                    "is_primary_key": True
                },
                {
                    "column_name": "name",
                    "data_type": "varchar",
                    "is_nullable": "NO",
                    "max_length": 200
                },
                {
                    "column_name": "price",
                    "data_type": "decimal",
                    "is_nullable": "NO",
                    "precision": 10,
                    "scale": 2
                }
            ]
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8820)