
# CKAN Sites Metadata Upload Script

This script uploads CKAN site instance metadata to POSE Ecosystem catalog using the `site` dataset type schema.


## CSV Column Quick Reference

| Field | Type | Required | Example |
|-------|------|----------|---------|
| name | slug | ✅ | `open-data-portal` |
| title | text | ✅ | `Open Data Portal` |
| notes | text | ✅ | `A portal for...` |
| ckan_version | text | ✅ | `2.10.5` |
| url | url | ❌ | `https://data.gov` |
| owner_org | text | ❌ | `government` |
| public_site | boolean | ❌ | `true` |
| location | text | ❌ | `Hamburg, Germany` |
| region | choice | ❌ | `Europe` |
| latitude | decimal | ❌ | `51.5074` |
| longitude | decimal | ❌ | `-0.1278` |
| site_type | choice | ❌ | `Government` |
| status | choice | ❌ | `production` |
| extensions | list | ❌ | `datastore,spatial` |
| tag_string | list | ❌ | `data,government` |
| language | code | ❌ | `en` |
| contact_email | email | ❌ | `admin@gov.org` |

## Common Values

### Region Options
- `North America`
- `Europe`
- `Asia-Pacific`
- `Latin America & Caribbean`
- `Global / Uncertain`

### Site Type Options
- `Government`
- `Research`
- `Educational`
- `Commercial`
- `Non_profit`
- `Unknown`

### Status Options
- `production`
- `staging`
- `development`

## Troubleshooting Cheatsheet

| Error | Solution |
|-------|----------|
| 403 Forbidden | Check API key |
| Organization not found | Create org first or omit |
| Invalid choice | Match exact schema values |
| URL already in use | Change `name` field |
| Missing required field | Add title/notes/ckan_version |

## Tips

✅ **DO:**
- Use lowercase slugs for `name`
- Test with 1-2 rows first
- Check organizations exist
- Use exact schema values

❌ **DON'T:**
- Use spaces in `name` field
- Mix up `name` (slug) and `title`
- Use invalid choice values
- Skip required fields


metadata CSV must include these columns (matching the site.yaml schema):

**Required Fields:**
- `name` - URL slug (lowercase, no spaces, e.g., "my-data-portal")
- `title` - Display name (e.g., "My Data Portal")
- `notes` - Description of the site
- `ckan_version` - CKAN version (e.g., "2.10.5")
- `owner_org` - Organization name (must exist in CKAN)

**Optional Fields:**

- `url` - Site URL
- `public_site` - true/false
- `location` - Location string (e.g., "Hamburg, Germany")
- `region` - Region from predefined choices
- `latitude` - Decimal latitude (e.g., "51.5074")
- `longitude` - Decimal longitude (e.g., "-0.1278")
- `site_type` - Commercial/Educational/Government/Non_profit/Research/Unknown
- `status` - production/staging/development
- `git_repo` - URL to git repository
- `extensions` - Comma-separated list of extensions
- `contact_email` - Maintainer email
- `tag_string` - Comma-separated tags
- `language` - Language code (e.g., "en", "es", "fr")
- `num_datasets` - Number of datasets (integer)
- `num_organizations` - Number of organizations (integer)
- `num_groups` - Number of groups (integer)
- `is_featured` - TRUE/FALSE
- `topic_id` - Topic identifier

**Screenshot Resource Fields:**
- `screenshot_url` - URL to screenshot image
- `screenshot_caption` - Caption for the screenshot
- `screenshot_path` - Local path to screenshot file (for upload)

### Step 3: Verify Organizations

If you're assigning sites to organizations:

1. Ensure the organizations exist in the catalog or else enter 'other'
2. Use the exact organization name (URL slug) in the `owner_org` column
3. If an organization doesn't exist, the site will be created without an organization

**To create organizations:**
- Via UI: Admin → Organizations → Add Organization
- Via API: Use `organization_create` action

### Step 4: Run the Script

```bash
python upload_sites.py
```

The script will:
- Process each row in the CSV file
- Wait 5 seconds between requests (rate limiting)
- Print JSON responses from CKAN
- Log progress and any errors
- Display a summary at the end

## CSV Field Details

### Boolean Fields

For fields like `public_site` and `is_featured`, use:
- `true`, `yes`, `1`, `y` for True
- `false`, `no`, `0`, `n` for False

### Multiple Values

**Extensions:** Comma-separated list
```
datastore,datapusher,spatial,scheming
```

**Tags:** Comma-separated list
```
government, open data, transparency
```

### Coordinates

Use decimal degrees format:
- Latitude: -90 to 90 (negative = South)
- longitude: -180 to 180 (negative = West)

Example:
```
latitude: 40.7128
longitude: -74.0060
```

### Predefined Choices

Ensure values match exactly:

**region:**
- North America
- Latin America & Caribbean
- Sub-Saharan Africa
- Global / Uncertain
- Europe
- Asia-Pacific
- North Africa & Middle East
- Central Asia

**site_type:**
- Commercial
- Educational
- Government
- Non_profit
- Research
- Unknown

**status:**
- production
- staging
- development

**language:** Use language codes (en, es, fr, de, pt, etc.)



## Troubleshooting

### Common Errors

**1. Authentication Error**
```
Error: 403 Forbidden
```
**Solution:** Check API key in `config.py`

**2. Organization Not Found**
```
Organization 'xyz' not found, creating site without organization
```
**Solution:** Create the organization first or leave `owner_org` empty

**3. Validation Error**
```
Error: Invalid value for field 'region'
```
**Solution:** Ensure the value exactly matches one of the choices in site.yaml

**4. Invalid Slug**
```
Error: That URL is already in use
```
**Solution:** Change the `name` field to a unique slug

**5. Missing Required Field**
```
Error: Missing value for required field 'title'
```
**Solution:** Ensure all required fields have values

### Rate Limiting

The script includes a 5-second delay between requests. To adjust:

Edit `upload_sites.py` and change:
```python
time.sleep(5)  # Change to desired seconds
```
