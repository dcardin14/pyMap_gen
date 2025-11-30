#!/usr/bin/env python3
"""
Build a small area map GeoPackage using PLSSID-based selection.

Data assumptions (from user description):
- PLSSID is a string like: CO230330N0190W0
  0-1  : state code (CO)
  2-3  : meridian code (e.g., 06, 23)  -> ignored
  4-7  : 4-digit township number, e.g. "0330"
  8    : township direction, e.g. "N" or "S"
  9-12 : 4-digit range number, e.g. "0190"
  13   : range direction, e.g. "E" or "W"
  14   : last char -> ignored

So T33N-R19W == PLSSID like CO??0330N0190W?.

Script steps:
1. Read config.map from CURRENT directory and parse township/range tokens like:
       T33N-R19W
   Optionally read a "County <Name>" line for output naming.

2. From ~/Dropbox/GIS/COLORADO:
   - ESPG26913_BLM_Colorado_Townships.gpkg
   - ESPG26913_BLM_Colorado_Sections.gpkg
   Select all features whose PLSSID matches any requested T/R.

3. Dissolve selected townships into a union geometry.

4. Clip:
   - ESPG26913_Larimer_Parcels.gpkg
   - ESPG26913_Weld_Parcels.gpkg
   by the township union.

5. Write all four layers into a single GeoPackage in current directory:
   - townships
   - sections
   - larimer_parcels
   - weld_parcels

You need:
    pip install geopandas fiona shapely pyproj
(ideally inside a virtualenv).
-----------------------------------------------------
# PLSSID layout (CadNSDI-style):
#
# 0-1   : State (CO)
# 2-3   : Meridian (ignored)
# 4-6   : Township number (3 digits)
# 7     : always zero (ignored)
# 8     : Township direction (N/S)
# 9-11  : Range number (3 digits)
# 12    : always zero (ignored)
# 13    : Range direction (E/W)
# 14    : unused / ignored

"""

import argparse
import sys
import re
from pathlib import Path

import geopandas as gpd

TEMPLATE_PROJECT = Path.home() / "Dropbox" / "GIS" / "AREA_MAP_TEMPLATE.qgz"
OUTPUT_PROJECT = Path.cwd() / "map.qgz"


# ----------------------------------------------------------------------
# CONFIG: adjust paths or filenames here if they differ on your system
# ----------------------------------------------------------------------

BASE_GIS_DIR = Path.home() / "Dropbox" / "GIS" / "COLORADO"

TOWNSHIP_GPKG = BASE_GIS_DIR / "ESPG26913_BLM_Colorado_Townships.gpkg"
SECTION_GPKG  = BASE_GIS_DIR / "ESPG26913_BLM_Colorado_Sections.gpkg"
LARIMER_GPKG  = BASE_GIS_DIR / "ESPG26913_Larimer_Parcels.gpkg"
WELD_GPKG     = BASE_GIS_DIR / "ESPG26913_Weld_Parcels.gpkg"

PLSSID_FIELD_NAME = "PLSSID"  # Townships field. Change this if your field is named differently
FRSTDIVID_FIELD_NAME = "FRSTDIVID" # Sections field. Change this if your field is named differently

# ----------------------------------------------------------------------


def parse_config_map(config_path: Path):
    """
    Parse config.map and return:
    county_name (or None),
    trs_list:      sorted list of unique (t_num, t_dir, r_num, r_dir) tuples,
    trs_sections:  dict[(t_num, t_dir, r_num, r_dir)] -> list[int] (section numbers)

    Accepts things like:
    County Larimer
    T33N-R19W Section 10 Section 12 Section 14
    T34N-R19W Section 1 Section 2 Section 3
    """
    if not config_path.exists():
        raise FileNotFoundError(f"config file not found: {config_path}")

    text = config_path.read_text(encoding="utf-8")

    # Normalize punctuation to spaces
    for ch in [":", ",", ";", "."]:
        text = text.replace(ch, " ")

    tokens = text.split()
    county = None
    trs_sections = {}   # key: (t_num, t_dir, r_num, r_dir) -> list of section ints

    i = 0
    tr_pattern = re.compile(r"^T(\d+)([NS])\-R(\d+)([EW])$", re.IGNORECASE)

    while i < len(tokens):
        tok = tokens[i]

        # Optional: "County Larimer"
        if tok.lower() == "county" and i + 1 < len(tokens):
            county = tokens[i + 1]
            i += 2
            continue

        m = tr_pattern.match(tok)
        if m:
            t_num = int(m.group(1))
            t_dir = m.group(2).upper()
            r_num = int(m.group(3))
            r_dir = m.group(4).upper()
            key = (t_num, t_dir, r_num, r_dir)

            # Ensure key exists
            if key not in trs_sections:
                trs_sections[key] = []

            i += 1
            # Zero or more "Section <num>" pairs
            while i < len(tokens) and tokens[i].lower() == "section":
                i += 1
                if i < len(tokens) and tokens[i].isdigit():
                    sec_num = int(tokens[i])
                    trs_sections[key].append(sec_num)
                    i += 1
                else:
                    break
            continue

        i += 1

    if not trs_sections:
        raise ValueError(
            "No township/range definitions found in config.map.\n"
            "Expected tokens like:  T33N-R19W Section 10 Section 12"
        )

    trs_list = sorted(trs_sections.keys())
    return county, trs_list, trs_sections



def filter_by_plssid_trs(gdf: gpd.GeoDataFrame, trs_list):
    """
    Given a GeoDataFrame with PLSSID, filter rows whose PLSSID
    matches any of the (t_num, t_dir, r_num, r_dir) tuples in trs_list.

    PLSSID format (user description + observed pattern):
      index:  0 1 2 3 4 5 6 7 8  9 10 11 12 13 14
      value:  C O 0 6 0 0 5 0 N  0  6  6  0  W  0
                        ^^^     ^  ^^^     ^
                        TTT     T  RRR     R

    - Township code is 4 digits: "TTT0"
    - Range code is 4 digits:    "RRR0"
    We match only the first 3 digits ("TTT" and "RRR") and ignore the trailing 0.
    """
    if PLSSID_FIELD_NAME not in gdf.columns:
        raise KeyError(
            f"Field '{PLSSID_FIELD_NAME}' not found in GeoDataFrame. "
            f"Available fields: {list(gdf.columns)}"
        )

    import pandas as pd  # local import to avoid top-level dependency issues

    # Ensure PLSSID is string
    pl = gdf[PLSSID_FIELD_NAME].astype(str)

    # Extract components from PLSSID
    # Township: chars [4:8] are "TTT0", we only care about [4:7] ("TTT")
    t_code3 = pl.str[4:7]          # 3-digit township number
    t_dir   = pl.str[8].str.upper()

    # Range: chars [9:13] are "RRR0", we only care about [9:12] ("RRR")
    r_code3 = pl.str[9:12]         # 3-digit range number
    r_dir   = pl.str[13].str.upper()

    # Start with all False mask
    mask_total = pd.Series(False, index=gdf.index)

    print("[DEBUG] Unique sample PLSSID values in this layer (up to 10):")
    for val in pl.dropna().unique()[:10]:
        print(f"         {val}")

    for (t_num, tdir, r_num, rdir) in trs_list:
        # Township / Range as 3-digit strings (no trailing zero)
        t_str = f"{t_num:03d}"
        r_str = f"{r_num:03d}"

        print(
            f"[DEBUG] Looking for T{t_num}{tdir}-R{r_num}{rdir} as:\n"
            f"        PLSSID[4:7]  = '{t_str}'  (township digits)\n"
            f"        PLSSID[8]    = '{tdir}'   (township dir)\n"
            f"        PLSSID[9:12] = '{r_str}'  (range digits)\n"
            f"        PLSSID[13]   = '{rdir}'   (range dir)"
        )

        mask = (
            (t_code3 == t_str) &
            (t_dir   == tdir)  &
            (r_code3 == r_str) &
            (r_dir   == rdir)
        )

        count = int(mask.sum())
        print(
            f"[INFO] T{t_num}{tdir}-R{r_num}{rdir}: matched {count} feature(s) by PLSSID"
        )

        if count > 0:
            matched_plssid = pl[mask].unique()[:10]
            print("[DEBUG]   Example matched PLSSID(s):")
            for val in matched_plssid:
                print(f"           {val}")

        mask_total |= mask

    selected = gdf[mask_total]
    print(f"[INFO] Total selected features in layer: {len(selected)}")
    return selected

def select_sections_by_trs_and_numbers(sections_gdf: gpd.GeoDataFrame, trs_sections):
    """
    Select specific sections using:
        - PLSSID for Township/Range (same logic as townships),
        - FRSTDIVID for Section number (last 5 chars: 'SNSS0', SS = 2-digit section).

    trs_sections: dict[(t_num, t_dir, r_num, r_dir)] -> list[int]
    """
    if PLSSID_FIELD_NAME not in sections_gdf.columns:
        raise KeyError(
            f"Field '{PLSSID_FIELD_NAME}' not found in sections layer. "
            f"Available fields: {list(sections_gdf.columns)}"
        )
    if FRSTDIVID_FIELD_NAME not in sections_gdf.columns:
        raise KeyError(
            f"Field '{FRSTDIVID_FIELD_NAME}' not found in sections layer. "
            f"Available fields: {list(sections_gdf.columns)}"
        )

    import pandas as pd

    pl = sections_gdf[PLSSID_FIELD_NAME].astype(str)
    fr = sections_gdf[FRSTDIVID_FIELD_NAME].astype(str)

    # Township and range from PLSSID (same trailing-zero pattern as before)
    t_code3 = pl.str[4:7]          # TTT
    t_dir   = pl.str[8].str.upper()
    r_code3 = pl.str[9:12]         # RRR
    r_dir   = pl.str[13].str.upper()

    # Section number from FRSTDIVID: last 5 chars = 'SNSS0' -> we need 'SS'
    sec_code2 = fr.str[-3:-1]      # 'SS'

    mask_total = pd.Series(False, index=sections_gdf.index)

    print("[DEBUG] Sample PLSSID/FRSTDIVID pairs (up to 5):")
    for v_pl, v_fr in list(zip(pl.head(), fr.head())):
        print(f"        PLSSID={v_pl}  FRSTDIVID={v_fr}")

    for (t_num, tdir, r_num, rdir), sec_list in trs_sections.items():
        if not sec_list:
            # No specific sections listed for this T/R, skip
            continue

        t_str = f"{t_num:03d}"
        r_str = f"{r_num:03d}"
        sec_strs = {f"{s:02d}" for s in sec_list}  # 2-digit section codes

        print(
            f"[DEBUG] Selecting sections for T{t_num}{tdir}-R{r_num}{rdir}, "
            f"sections {sec_list} as:\n"
            f"        PLSSID[4:7]  = '{t_str}' (T)\n"
            f"        PLSSID[8]    = '{tdir}'  (T dir)\n"
            f"        PLSSID[9:12] = '{r_str}' (R)\n"
            f"        PLSSID[13]   = '{rdir}'  (R dir)\n"
            f"        FRSTDIVID[-3:-1] in {sorted(sec_strs)} (section)"
        )

        mask = (
            (t_code3 == t_str) &
            (t_dir   == tdir)  &
            (r_code3 == r_str) &
            (r_dir   == rdir)  &
            (sec_code2.isin(sec_strs))
        )

        count = int(mask.sum())
        print(
            f"[INFO] T{t_num}{tdir}-R{r_num}{rdir} sections {sec_list}: "
            f"matched {count} feature(s)"
        )   

        if count > 0:
            matched_ids = fr[mask].unique()[:10]
            print("[DEBUG]   Example matched FRSTDIVID(s):")
            for val in matched_ids:
                print(f"           {val}")

        mask_total |= mask

    selected = sections_gdf[mask_total]
    print(f"[INFO] Total selected sections: {len(selected)}")
    return selected

def dissolve_townships_for_clip(townships: gpd.GeoDataFrame):
    """
    Dissolve township polygons into a single (possibly multipart) geometry
    for use as a clip mask.
    """
    if townships.empty:
        raise RuntimeError("No township features selected; nothing to dissolve for clipping.")

    print("[INFO] Dissolving township polygons for clip mask...")
    dissolved = townships.dissolve()
    # In case of multipart, explode, but we can still pass entire gdf to clip.
    dissolved = dissolved.explode(index_parts=False)
    return dissolved


def clip_parcels(parcel_gpkg: Path, clip_gdf: gpd.GeoDataFrame, label: str):
    """
    Clip parcels from parcel_gpkg by clip_gdf. Returns clipped GeoDataFrame.
    """
    print(f"[INFO] Reading parcels from {parcel_gpkg}")
    parcels = gpd.read_file(parcel_gpkg)

    if parcels.empty:
        print(f"[WARN] {label} parcels layer is empty.")
        return parcels

    if clip_gdf.crs != parcels.crs:
        print("[INFO] Reprojecting clip layer to match parcel CRS...")
        clip_gdf = clip_gdf.to_crs(parcels.crs)

    print(f"[INFO] Clipping {label} parcels...")
    clipped = gpd.clip(parcels, clip_gdf)
    print(f"[INFO]   {label}: {len(clipped)} parcel(s) after clip.")
    return clipped


def main():
    parser = argparse.ArgumentParser(
        description="Create a local area GeoPackage by selecting PLSS townships via PLSSID and clipping parcels."
    )
    parser.add_argument(
        "--config",
        default="config.map",
        help="Config file in the current directory (default: config.map)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output GeoPackage name (default: derived from county or area_map.gpkg)",
    )
    args = parser.parse_args()

    cwd = Path.cwd()
    config_path = cwd / args.config

    print(f"[INFO] Using config file: {config_path}")
    county, trs_list, trs_sections = parse_config_map(config_path)

    print(f"[INFO] Parsed {len(trs_list)} township/range entries from config:")
    for t_num, tdir, r_num, rdir in trs_list:
        secs = trs_sections.get((t_num, tdir, r_num, rdir), [])
        if secs:
            print(f"       T{t_num}{tdir}-R{r_num}{rdir}  Sections: {secs}")
        else:
            print(f"       T{t_num}{tdir}-R{r_num}{rdir}  Sections: (none listed / ALL ignored)")
    if county:
        print(f"[INFO] County: {county}")

    # --- Select townships by PLSSID (T/R only) ---
    print(f"[INFO] Reading townships from {TOWNSHIP_GPKG}")
    townships_all = gpd.read_file(TOWNSHIP_GPKG)
    townships_sel = filter_by_plssid_trs(townships_all, trs_list)

    if townships_sel.empty:
        raise RuntimeError("No townships matched the requested TR list via PLSSID.")

    # --- Select SPECIFIC sections using PLSSID + FRSTDIVID ---
    print(f"[INFO] Reading sections from {SECTION_GPKG}")
    sections_all = gpd.read_file(SECTION_GPKG)

    # All sections in the selected townships (T/R only)
    sections_all_tr = filter_by_plssid_trs(sections_all, trs_list)

    if sections_all_tr.empty:
        print("[WARN] No sections matched the requested TR list via PLSSID.")
    else:
        print(f"[INFO] Sections in selected townships: {len(sections_all_tr)}")

    # Specific sections only (T/R + FRSTDIVID-based section numbers)
    sections_sel = select_sections_by_trs_and_numbers(sections_all_tr, trs_sections)


    # --- Build clip mask from dissolved townships ---
    townships_union = dissolve_townships_for_clip(townships_sel)
    
    #--- ALSO build a clip mask from dissolved sections ---
    if sections_sel.empty:
        print("[WARN] No sections selected; will skip section-based parcel clips.")
        sections_union = None
    else:
        print("[INFO] Dissolving sections into clip mask...")
        sections_union = dissolve_townships_for_clip(sections_sel)  # same logic, works fine
    
    # --- Clip parcels by TOWNSHIPS---
    larimer_clipped = clip_parcels(LARIMER_GPKG, townships_union, "Larimer")
    weld_clipped    = clip_parcels(WELD_GPKG,    townships_union, "Weld")

    # --- NEW: Clip parcels by SECTIONS (more specific) ---
    if sections_union is not None:
        larimer_sections_clipped = clip_parcels(
            LARIMER_GPKG, sections_union, "Larimer (sections)"
        )
        weld_sections_clipped = clip_parcels(
            WELD_GPKG, sections_union, "Weld (sections)"
        )
    else:
        # Create empty frames so later code doesn't blow up
        larimer_sections_clipped = gpd.GeoDataFrame(geometry=[], crs=larimer_clipped.crs if not larimer_clipped.empty else None)
        weld_sections_clipped    = gpd.GeoDataFrame(geometry=[], crs=weld_clipped.crs if not weld_clipped.empty else None)
        
    # --- Determine output path ---
    if args.output:
        out_path = cwd / args.output
    else:
        base_name = "area_map"
        if county:
            base_name = f"{county}_area_map"
        out_path = cwd / f"{base_name}.gpkg"

    if out_path.exists():
        print(f"[INFO] Output file {out_path} already exists. It will be overwritten.")
        out_path.unlink()

    print(f"[INFO] Writing combined GeoPackage to {out_path}")

    # Write all layers into one GPKG
    townships_sel.to_file(out_path, layer="townships", driver="GPKG", mode="w")

    # 1) All sections in selected townships
    if not sections_all_tr.empty:
        sections_all_tr.to_file(out_path, layer="sections", driver="GPKG", mode="a")
    else:
        print("[WARN] No township-wide sections found; 'sections' layer will be omitted.")

    # 2) Only your specific sections
    if not sections_sel.empty:
        sections_sel.to_file(out_path, layer="sections_target", driver="GPKG", mode="a")
    else:
        print("[WARN] No specific sections selected; 'sections_target' layer will be omitted.")

    if not larimer_clipped.empty:
        larimer_clipped.to_file(out_path, layer="larimer_parcels", driver="GPKG", mode="a")
    else:
        print("[WARN] No Larimer parcels in clip area; 'larimer_parcels' will be empty or omitted.")

    if not weld_clipped.empty:
        weld_clipped.to_file(out_path, layer="weld_parcels", driver="GPKG", mode="a")
    else:
        print("[WARN] No Weld parcels in clip area; 'weld_parcels' will be empty or omitted.")
        
    # NEW: write section-clipped parcel layers
    if not larimer_sections_clipped.empty:
        larimer_sections_clipped.to_file(
            out_path,
            layer="larimer_parcels_sections",
            driver="GPKG",
            mode="a",
        )
    else:
        print("[WARN] No Larimer parcels in section clip area.")

    if not weld_sections_clipped.empty:
        weld_sections_clipped.to_file(
            out_path,
            layer="weld_parcels_sections",
            driver="GPKG",
            mode="a",
        )
    else:
        print("[WARN] No Weld parcels in section clip area.")    

    print("[DONE] Wrote GeoPackage with selected PLSS + clipped parcels.")
    print(f"[DONE] Output: {out_path}")
    
    print("\nSUMMARY:")
    print(f"  Townships selected:            {len(townships_sel)}")
    print(f"  Sections selected:             {len(sections_sel)}")
    print(f"  Larimer parcels (townships):   {len(larimer_clipped)}")
    print(f"  Weld parcels (townships):      {len(weld_clipped)}")
    print(f"  Larimer parcels (sections):    {len(larimer_sections_clipped)}")
    print(f"  Weld parcels (sections):       {len(weld_sections_clipped)}")

    print("[DONE] Wrote GeoPackage with selected PLSS + clipped parcels.")
    print(f"[DONE] Output: {out_path}")
    
    import shutil

    # --- Copy template project into working directory ---
    if not TEMPLATE_PROJECT.exists():
        print(f"[ERROR] Template project not found: {TEMPLATE_PROJECT}")
    else:
        shutil.copy(TEMPLATE_PROJECT, OUTPUT_PROJECT)
        print(f"[INFO] Copied template → {OUTPUT_PROJECT}")

    import subprocess
    subprocess.Popen(["qgis", str(out_path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
