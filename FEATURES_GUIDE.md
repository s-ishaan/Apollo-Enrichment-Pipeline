# New Features Guide

## 1. Upload Base Data (Without Apollo Enrichment)

### Location
Navigate to **📥 Upload Base Data** in the sidebar

### Purpose
Upload your existing Excel database directly to the system **without calling Apollo API**. This is perfect for:
- Importing your historical contact data
- Bulk uploading from other CRM systems
- Merging data from multiple sources
- Loading your baseline database before enriching new contacts

### How It Works
1. Select "📥 Upload Base Data" from the sidebar
2. Upload your Excel file (.xlsx)
3. Preview the data
4. Click "Upload to Database"
5. Your data is loaded directly (no API calls, no enrichment)

### What Happens
- ✅ Excel parsed and columns mapped (same flexible detection as enrichment upload)
- ✅ Data normalized (emails validated, whitespace cleaned, domains extracted)
- ✅ Duplicates removed (by email)
- ✅ Data saved to database
- ❌ **No Apollo API calls** (no enrichment)
- ❌ **No API key required** for this page

### Use Cases
**Scenario 1: Initial Database Load**
- You have 5,000 contacts in an old CRM
- Upload them via "Upload Base Data"
- Later, upload new contacts via "Upload & Enrich" to enrich only new leads

**Scenario 2: Partial Enrichment**
- Upload base data first
- Then use "Upload & Enrich" to add Apollo data to specific segments
- Saves API credits by not re-enriching existing data

---

## 2. Enhanced Search & Filter in Database Viewer

### Location
Navigate to **🗄️ Database Viewer** in the sidebar

### Available Filters

#### Basic Filters
- **Email** - Search by email address
- **Company Name** - Search by company
- **Country** - Search by country

#### Person Filters
- **First Name** - Search by first name
- **Last Name** - Search by last name
- **Job Title** - Search by job title (e.g., "CEO", "Manager", "Engineer")

#### Company Filters
- **Industry** - Search by industry (e.g., "Technology", "Finance")
- **State** - Search by state/province
- **Website** - Search by website domain

#### Lead Filters
- **Lead Source** - Search by source (e.g., "Excel Upload", "Web Form")
- **Client Type** - Search by client type
- **Email Send Status** - Filter by "Yes", "No", or "All"

### How to Use

1. Go to **🗄️ Database Viewer**
2. Click the **🔍 Search & Filter** expander
3. Enter search terms in any combination of filters
4. Results update automatically
5. Use pagination to browse through results

### Search Features

- **Partial matching** - Search "john" to find "john@example.com", "johnsmith@test.com"
- **Case insensitive** - "GOOGLE" matches "Google", "google", "GOOGLE"
- **Multiple filters** - Combine filters (e.g., "Country: USA" + "Industry: Technology")
- **Real-time results** - Count updates as you type

### Example Searches

**Find all CEOs in California:**
- Job Title: "CEO"
- State: "California"

**Find all contacts from tech companies:**
- Industry: "Technology"

**Find specific person:**
- First Name: "John"
- Last Name: "Doe"
- Company Name: "Acme"

**Find all contacts ready for email:**
- Email Send Status: "Yes"

---

## 3. Complete Workflow Examples

### Workflow 1: Loading Historical Data + Enriching New Leads

1. **Upload your existing database:**
   - Go to **📥 Upload Base Data**
   - Upload your Excel with 5,000 existing contacts
   - ✅ All data loaded to database (no API calls)

2. **Enrich new leads:**
   - Go to **📤 Upload & Enrich**
   - Upload new leads (100 contacts)
   - ✅ These get enriched via Apollo
   - ✅ All data merged in single database

3. **Search and filter:**
   - Go to **🗄️ Database Viewer**
   - Filter by "Lead Source: Excel Upload" to see base data
   - Filter by "Apollo Person: Email Status: verified" to see enriched leads

### Workflow 2: Targeted Enrichment

1. **Load everything as base data:**
   - Upload 10,000 contacts via **📥 Upload Base Data**

2. **Export specific segment:**
   - Go to **🗄️ Database Viewer**
   - Filter: "Job Title: CEO" + "Country: USA"
   - Export to Excel

3. **Enrich the segment:**
   - Upload the exported file via **📤 Upload & Enrich**
   - Only CEOs get enriched (saves API credits)

### Workflow 3: Data Consolidation

1. **Upload data from CRM 1:**
   - **📥 Upload Base Data** → upload.xlsx

2. **Upload data from CRM 2:**
   - **📥 Upload Base Data** → upload2.xlsx
   - Duplicates automatically merged by email

3. **Upload new leads with enrichment:**
   - **📤 Upload & Enrich** → new_leads.xlsx
   - New leads enriched and added

4. **Search across all sources:**
   - **🗄️ Database Viewer** → Filter and search all data

---

## Tips & Best Practices

### When to Use Base Upload vs Enrichment Upload

**Use Base Upload (📥) when:**
- You have historical data that doesn't need enrichment
- You want to save API credits
- You're importing from another system
- You already have complete contact information

**Use Enrichment Upload (📤) when:**
- You have new leads that need enrichment
- You want to add Apollo data (job titles, company info, etc.)
- You want to verify emails
- You need up-to-date contact information

### Search Tips

1. **Use multiple filters for precision:**
   - Bad: Search "john" (returns 1000s of results)
   - Good: "First Name: john" + "Company: Google" + "Job Title: Engineer"

2. **Use partial matches:**
   - Search "tech" in Industry to find "Technology", "FinTech", "BioTech"

3. **Export filtered results:**
   - Apply filters
   - Click "Export to Excel"
   - Get only the filtered subset

### Database Management

1. **Regular deduplication:**
   - Upload same file via Base Upload
   - Duplicates automatically updated (not duplicated)

2. **Incremental updates:**
   - Upload new data anytime
   - Email-based matching prevents duplicates
   - Timestamps track latest updates

3. **Hybrid approach:**
   - Use Base Upload for bulk data
   - Use Enrichment Upload for high-value leads
   - Balance API usage with data needs

---

## Technical Details

### Column Mapping (Both Upload Types)

Both base and enrichment uploads use the same flexible column detection:

| Your Excel Column | Maps To |
|---|---|
| Email, E-mail, Email Address | Email ID (unique) |
| First Name, FirstName, fname | First Name |
| Last Name, LastName, lname | Last Name |
| Company, Organization, Org | Company Name (Based on Website Domain) |
| Website, Domain, URL | Website URLs |
| Phone, Contact Number | Contact Number (Person) |
| Title, Job Title, Position | Job Title |

### Data Processing

**Base Upload:**
1. Parse Excel ✅
2. Map columns ✅
3. Normalize data ✅
4. Validate emails ✅
5. Deduplicate ✅
6. Save to database ✅
7. ~~Apollo enrichment~~ ❌

**Enrichment Upload:**
1. Parse Excel ✅
2. Map columns ✅
3. Normalize data ✅
4. Validate emails ✅
5. Deduplicate ✅
6. Apollo people enrichment ✅
7. Apollo company enrichment ✅
8. Save to database ✅

### Database Behavior

- **Primary key:** Email ID (unique)
- **On duplicate:** UPDATE existing record (S.N. preserved)
- **On new:** INSERT with new S.N. (auto-increment)
- **Timestamp:** "UPDATE AS ON" set to current UTC time
- **Lead Source:** "Excel Upload" (default)

---

## Troubleshooting

### "No records found" in Database Viewer
- Check if you have any filters active
- Clear all filters and try again
- Verify data was uploaded successfully

### "Duplicate key" error
- This shouldn't happen (email is unique key)
- If it does, there's a bug - contact support

### Base upload not working
- Check Excel file format (.xlsx only)
- Verify at least Email column exists
- Check logs: `apollo_pipeline.log`

### Search not finding records
- Try partial match (e.g., "goo" instead of "google")
- Check spelling
- Search is case-insensitive but requires partial match

---

## Summary

✅ **Two upload modes:** Base (fast, no API) and Enrichment (with Apollo)
✅ **Enhanced search:** 12 different filters for precise queries
✅ **Flexible workflow:** Load base data, enrich selectively
✅ **No duplicates:** Email-based deduplication
✅ **Export anywhere:** Filter and export subsets

Your database is now a true single source of truth!
