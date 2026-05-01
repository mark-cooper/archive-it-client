# Transform raw Archive-It responses into deterministic fake fixture data.
#
# Raw API responses are stored only in fixtures.local/ and are gitignored.
# Committed fixtures preserve response shape for serde tests but should contain
# no real-world values.
#
# Usage: jq -f fakeify.jq input.json

def fake_string($key):
  if ($key | test("(^|[_-])(date|time)($|[_-])|(^|[_-])crawl-start$"; "i")) then "1970-01-01T00:00:00Z"
  elif ($key | test("url|uri|link|location"; "i")) then "https://example.invalid/fake"
  elif ($key | test("email"; "i")) then "fake@example.invalid"
  elif ($key | test("token"; "i")) then "FAKE_TOKEN"
  elif ($key | test("sha1"; "i")) then "FAKE_SHA1"
  elif ($key | test("md5"; "i")) then "FAKE_MD5"
  elif ($key | test("filename"; "i")) then "ARCHIVEIT-FAKE-19700101000000-00000-fake.warc.gz"
  elif ($key | test("filetype"; "i")) then "warc"
  elif ($key | test("state"; "i")) then "ACTIVE"
  elif ($key | test("organization_name"; "i")) then "Fake Organization"
  elif ($key | test("description"; "i")) then "Fake description."
  elif ($key | test("name"; "i")) then "Fake Name"
  else "FAKE"
  end;

def fake_number($key):
  if $key == "count" then 1
  else 0
  end;

def fake_value($key):
  if . == null then null
  elif type == "string" then fake_string($key)
  elif type == "number" then fake_number($key)
  elif type == "boolean" then false
  elif type == "array" then
    if length == 0 then []
    else [.[0] | fake_value($key)]
    end
  elif type == "object" then
    if $key == "metadata" then
      if length > 0 then {"fake_metadata": ([.[]][0] | fake_value("fake_metadata"))}
      else {}
      end
    else with_entries(. as $entry | .value = ($entry.value | fake_value($entry.key)))
    end
  else .
  end;

fake_value("")
