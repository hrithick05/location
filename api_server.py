"""
FastAPI server for e-commerce product data
Handles parsing HTML files and serving product data as JSON
Supports: D-Mart, JioMart, Nature's Basket, Zepto, and Swiggy Instamart
"""

from fastapi import FastAPI, File, UploadFile, HTTPException, Body
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import json
import os
import subprocess
import asyncio
from pathlib import Path
import uvicorn

app = FastAPI(
    title="E-commerce Product Parser API",
    description="API for parsing and serving product data from e-commerce sites (D-Mart, JioMart, Nature's Basket, Zepto, Swiggy Instamart)",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data models
class Product(BaseModel):
    name: str
    mrp: Optional[float] = None
    price: Optional[float] = None
    discount: Optional[float] = None
    discountAmount: Optional[float] = None
    isOutOfStock: bool = False

class ProductData(BaseModel):
    site: str
    location: str
    products: List[Product]
    totalProducts: int
    filename: str

class ParseRequest(BaseModel):
    location: str
    product: str

class ScrapeRequest(BaseModel):
    location: str
    product: str

# Directory paths
OUTPUTS_DIR = Path("outputs")
OUTPUTS_DIR.mkdir(exist_ok=True)

# Supported sites
SUPPORTED_SITES = ["dmart", "jiomart", "naturesbasket", "zepto", "swiggy"]


@app.get("/")
async def root():
    """Root endpoint - API information"""
    return {
        "message": "E-commerce Product Parser API",
        "version": "1.0.0",
        "supported_sites": SUPPORTED_SITES,
        "endpoints": {
            "GET /api/products": "Get all parsed products",
            "GET /api/products/{site}": "Get products by site",
            "GET /api/products/{site}/{location}": "Get products by site and location",
            "POST /api/parse": "Parse HTML file and return JSON",
            "POST /api/scrape": "Scrape products from all sites",
            "POST /api/upload": "Upload and parse HTML file",
            "GET /api/health": "Health check endpoint"
        }
    }


@app.get("/api/products")
async def get_all_products():
    """Get all parsed products from all sites"""
    try:
        results = []
        
        # Find all JSON files in outputs directory
        json_files = list(OUTPUTS_DIR.glob("*-parsed.json"))
        
        if not json_files:
            return {
                "message": "No parsed data found",
                "products": [],
                "total": 0
            }
        
        # Load all JSON files
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        results.extend(data)
                    else:
                        results.append(data)
            except Exception as e:
                print(f"Error reading {json_file}: {e}")
                continue
        
        # Aggregate all products
        all_products = []
        sites = {}
        
        for result in results:
            site = result.get('site', 'unknown')
            location = result.get('location', 'Unknown')
            products = result.get('products', [])
            
            if site not in sites:
                sites[site] = {
                    'location': location,
                    'products': [],
                    'total': 0
                }
            
            sites[site]['products'].extend(products)
            sites[site]['total'] += len(products)
            all_products.extend(products)
        
        return {
            "message": "Success",
            "sites": sites,
            "all_products": all_products,
            "total_products": len(all_products),
            "total_sites": len(sites)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving products: {str(e)}")


@app.get("/api/products/{site}")
async def get_products_by_site(site: str):
    """Get products for a specific site (dmart, jiomart, naturesbasket, zepto, swiggy)"""
    try:
        site = site.lower()
        
        # Validate site
        if site not in SUPPORTED_SITES:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported site: {site}. Supported sites: {', '.join(SUPPORTED_SITES)}"
            )
        
        results = []
        
        # Find JSON files for this site
        # Try multiple patterns to catch different filename formats
        json_files = list(OUTPUTS_DIR.glob(f"{site}*-parsed.json"))
        json_files.extend(list(OUTPUTS_DIR.glob(f"*{site}*-parsed.json")))
        
        if not json_files:
            return {
                "message": f"No data found for site: {site}",
                "site": site,
                "products": [],
                "total": 0
            }
        
        # Load JSON files
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        results.extend([r for r in data if r.get('site', '').lower() == site])
                    elif data.get('site', '').lower() == site:
                        results.append(data)
            except Exception as e:
                print(f"Error reading {json_file}: {e}")
                continue
        
        if not results:
            return {
                "message": f"No data found for site: {site}",
                "site": site,
                "products": [],
                "total": 0
            }
        
        # Aggregate products
        all_products = []
        locations = {}
        
        for result in results:
            location = result.get('location', 'Unknown')
            products = result.get('products', [])
            
            if location not in locations:
                locations[location] = []
            
            locations[location].extend(products)
            all_products.extend(products)
        
        return {
            "message": "Success",
            "site": site,
            "locations": locations,
            "products": all_products,
            "total_products": len(all_products),
            "total_locations": len(locations)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving products: {str(e)}")


@app.get("/api/products/{site}/{location}")
async def get_products_by_site_and_location(site: str, location: str):
    """Get products for a specific site and location"""
    try:
        site = site.lower()
        location = location.lower()
        
        # Validate site
        if site not in SUPPORTED_SITES:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported site: {site}. Supported sites: {', '.join(SUPPORTED_SITES)}"
            )
        
        # Find JSON files for this site
        json_files = list(OUTPUTS_DIR.glob(f"{site}*-parsed.json"))
        json_files.extend(list(OUTPUTS_DIR.glob(f"*{site}*-parsed.json")))
        
        if not json_files:
            return {
                "message": f"No data found for site: {site}",
                "site": site,
                "location": location,
                "products": [],
                "total": 0
            }
        
        # Load and filter JSON files
        matching_results = []
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    results = data if isinstance(data, list) else [data]
                    
                    for result in results:
                        result_site = result.get('site', '').lower()
                        result_location = result.get('location', '').lower()
                        
                        if (result_site == site and location in result_location):
                            matching_results.append(result)
            except Exception as e:
                print(f"Error reading {json_file}: {e}")
                continue
        
        if not matching_results:
            return {
                "message": f"No data found for site: {site}, location: {location}",
                "site": site,
                "location": location,
                "products": [],
                "total": 0
            }
        
        # Aggregate products
        all_products = []
        for result in matching_results:
            all_products.extend(result.get('products', []))
        
        return {
            "message": "Success",
            "site": site,
            "location": location,
            "products": all_products,
            "total_products": len(all_products)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving products: {str(e)}")


@app.post("/api/parse")
async def parse_html_file(file_path: str = Body(..., embed=True)):
    """Parse an HTML file and return JSON data"""
    try:
        # Resolve file path
        html_path = Path(file_path)
        if not html_path.exists():
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
        
        if not html_path.suffix == '.html':
            raise HTTPException(status_code=400, detail="File must be an HTML file")
        
        # Run the unified-html-parser.js
        process = await asyncio.create_subprocess_exec(
            'node', 'unified-html-parser.js', str(html_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown error"
            raise HTTPException(status_code=500, detail=f"Parsing failed: {error_msg}")
        
        # Find the generated JSON file
        json_file = OUTPUTS_DIR / f"{html_path.stem}-parsed.json"
        
        if not json_file.exists():
            raise HTTPException(status_code=500, detail="JSON file was not generated")
        
        # Load and return JSON
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return {
            "message": "Successfully parsed HTML file",
            "data": data
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing file: {str(e)}")


@app.post("/api/upload")
async def upload_and_parse(file: UploadFile = File(...)):
    """Upload an HTML file and parse it"""
    try:
        if not file.filename.endswith('.html'):
            raise HTTPException(status_code=400, detail="File must be an HTML file")
        
        # Save uploaded file
        upload_path = OUTPUTS_DIR / file.filename
        with open(upload_path, 'wb') as f:
            content = await file.read()
            f.write(content)
        
        # Parse the file
        process = await asyncio.create_subprocess_exec(
            'node', 'unified-html-parser.js', str(upload_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown error"
            raise HTTPException(status_code=500, detail=f"Parsing failed: {error_msg}")
        
        # Find the generated JSON file
        json_file = OUTPUTS_DIR / f"{upload_path.stem}-parsed.json"
        
        if not json_file.exists():
            raise HTTPException(status_code=500, detail="JSON file was not generated")
        
        # Load and return JSON
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return {
            "message": "Successfully uploaded and parsed HTML file",
            "filename": file.filename,
            "data": data
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.post("/api/scrape")
async def scrape_products(request: ScrapeRequest):
    """Scrape products from all e-commerce sites (D-Mart, JioMart, Nature's Basket, Zepto, Swiggy Instamart)"""
    try:
        location = request.location
        product = request.product
        
        # Run the orchestrator (it runs on all sites sequentially)
        process = await asyncio.create_subprocess_exec(
            'node', 'location-selector-orchestrator.js', product, location,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown error"
            # Don't fail completely - some sites might have succeeded
            print(f"Warning: Scraping had errors: {error_msg}")
        
        # Wait a bit for files to be written
        await asyncio.sleep(2)
        
        # Parse all generated HTML files from output directory
        output_dir = Path("output")
        html_files = []
        
        if output_dir.exists():
            html_files = list(output_dir.glob("*.html"))
        
        if not html_files:
            return {
                "message": "Scraping completed but no HTML files found",
                "location": location,
                "product": product,
                "data": [],
                "sites_scraped": []
            }
        
        # Parse all HTML files
        parse_process = await asyncio.create_subprocess_exec(
            'node', 'unified-html-parser.js', str(output_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        parse_stdout, parse_stderr = await parse_process.communicate()
        
        # Wait a bit for JSON files to be written
        await asyncio.sleep(1)
        
        # Find the latest combined JSON file
        json_files = sorted(OUTPUTS_DIR.glob("parsed-results-*.json"), key=os.path.getmtime, reverse=True)
        
        if json_files:
            with open(json_files[0], 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract sites that were successfully scraped
            sites_scraped = []
            if isinstance(data, list):
                sites_scraped = [item.get('site', 'unknown') for item in data if item.get('totalProducts', 0) > 0]
            elif isinstance(data, dict):
                if data.get('totalProducts', 0) > 0:
                    sites_scraped = [data.get('site', 'unknown')]
            
            return {
                "message": "Successfully scraped and parsed products",
                "location": location,
                "product": product,
                "data": data,
                "total_sites": len(data) if isinstance(data, list) else 1,
                "sites_scraped": list(set(sites_scraped))
            }
        else:
            # Fallback: load individual JSON files
            individual_files = list(OUTPUTS_DIR.glob("*-parsed.json"))
            all_data = []
            sites_scraped = []
            
            for json_file in individual_files:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            all_data.extend(data)
                            sites_scraped.extend([item.get('site', 'unknown') for item in data if item.get('totalProducts', 0) > 0])
                        else:
                            all_data.append(data)
                            if data.get('totalProducts', 0) > 0:
                                sites_scraped.append(data.get('site', 'unknown'))
                except Exception as e:
                    print(f"Error reading {json_file}: {e}")
                    continue
            
            return {
                "message": "Successfully scraped and parsed products",
                "location": location,
                "product": product,
                "data": all_data,
                "total_sites": len(all_data),
                "sites_scraped": list(set(sites_scraped))
            }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error scraping products: {str(e)}")


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    # Check if required files exist
    required_files = {
        "unified-html-parser.js": Path("unified-html-parser.js").exists(),
        "location-selector-orchestrator.js": Path("location-selector-orchestrator.js").exists()
    }
    
    return {
        "status": "healthy",
        "outputs_directory": str(OUTPUTS_DIR),
        "outputs_exists": OUTPUTS_DIR.exists(),
        "output_directory": str(Path("output")),
        "output_exists": Path("output").exists(),
        "required_files": required_files,
        "supported_sites": SUPPORTED_SITES
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

