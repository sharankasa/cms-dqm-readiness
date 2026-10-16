# cms-dqm-readiness

Code and aggregate results for a preregistered study of two of CMS's new FHIR quality measures on real hospital
data: CMS816 (severe hypoglycemia) and CMS871 (severe hyperglycemia), run over all 431,231 hospital stays in
MIMIC-IV-on-FHIR.

The study asked two things. Can the measures be run over a whole hospital database without changing what CMS's
engine would report? And which problems in the data change the answer?

- **Paper:** *Identical results, hidden data problems: running CMS's digital quality measures on real hospital data* (preprint link to be added)
- **Preregistration:** https://osf.io/jhkvt (doi:[10.17605/OSF.IO/JHKVT](https://doi.org/10.17605/OSF.IO/JHKVT))
- **Author:** Sai Sharan Kasa, Independent Researcher, Seattle, WA ([ORCID 0009-0002-7425-4516](https://orcid.org/0009-0002-7425-4516))

## What the study found

**Running the measures at scale is not the hard part.** Each patient's record is cut down to the slice of data around
one hospital stay, and the unchanged engine (fqm-execution, the engine CMS's authoring tool uses) runs on that slice.
On a 2,000-stay test sample this gave the same result as the engine on whole records for every stay compared, while
whole records needed about a hundred times more computing for CMS816. All 431,231 stays then ran in 5.6 hours on
eight small cloud machines, for about 1 cent of computing per 1,000 stays.

**The data decide what the measures report.**
- Bedside glucose readings recorded without a unit cannot be compared with the measures' mg/dL thresholds. In the
  2017–2019 cohort that hid 44% of the severe hypoglycemic events (57 of 130). The unit is missing in the hospital's own
  charting, not only in the FHIR export.
- Medication mapping choices moved hundreds of stays in and out of the measures while the rates barely changed.
- Losing the link between a stay and its diabetes diagnosis removed stays without events and so raised the
  hyperglycemia rate. Removing bedside tests raised it too.
- Records with every one of these problems still pass FHIR profile validation. Only checks built from the measures'
  own logic found them.

## What is in this repository

| Folder | What it does |
|---|---|
| `pipeline/prepare` | Maps MIMIC-IV-on-FHIR to the measures' codes and profiles and cuts each patient's record into one slice per stay (SQL that runs the same on DuckDB and Amazon Athena) |
| `pipeline/worker`, `infra/eks` | Runs the unchanged engine on slices in parallel on a short-lived Kubernetes cluster, with a cost watchdog |
| `pipeline/s1` | The equivalence test: slices against whole records, stay by stay |
| `pipeline/s2` | Runtime and cost: repeated timing runs, 1 to 8 nodes, the full run |
| `pipeline/readiness` | Readiness checks generated from the measures' data requirements, problem injection, and re-running the measures under each mapping variant and data scenario |
| `pipeline/v` | An independent Python implementation of both measures, written from CMS's specification text |
| `pipeline/cohort`, `pipeline/r3`, `pipeline/terminology` | Year linkage and the 2020–2022 check, completeness across eICU hospitals, and the medication mapping |
| `testcases` | Runs CMS's published test cases through the engine |
| `manuscript` | Scripts that rebuild the paper's tables and figures from `results/`, and the generated tables and figures |
| `results` | Aggregate results behind every number in the paper |

The short folder names (s1, s2, v, r3) follow the outcome labels in the preregistration.

## What is not here

- **No patient data.** MIMIC-IV, MIMIC-IV-on-FHIR and eICU are available to credentialed users from
  [PhysioNet](https://physionet.org) under their data use agreements. Everything in `results/` is aggregate; counts
  from 1 to 9 are shown as `<10`.
- **No value-set expansions.** They come from the Value Set Authority Center and need a free UMLS license
  (`testcases/fetch_vsac.py` downloads them with your own key).
- **No medication crosswalk or review decisions.** They were built from the credentialed data and contain its drug
  names. The SQL that builds and applies them is included.

## Running it

**On open data, without credentials.** CMS's test cases (`testcases/run_fqm.js`) and the equivalence test on the open
MIMIC-IV-on-FHIR demonstration data (`pipeline/s1/run_s1_demo.sh`) run on one workstation with Node.js, Python and
Docker.

**At full scale.** You need PhysioNet access to the datasets, an AWS account, the data copied to your own S3 bucket and
registered in Athena, and Terraform for the cluster. The scripts use the placeholder `your-bucket` for the bucket name;
replace it with yours. Each script explains its own steps at the top of the file. The cluster in `infra/eks` is meant
to be created for a session and destroyed afterwards; its watchdog stops all nodes at a cost cap or a maximum age.

## Rebuilding the tables and figures

```
python3 -m pip install -r requirements-figures.txt
python3 manuscript/scripts/make_tables_1_2_s2_figures_2_4.py
python3 manuscript/scripts/make_table3_figure5.py
python3 manuscript/scripts/make_figure6_scale.py
python3 manuscript/scripts/make_figures_1_3.py
python3 manuscript/scripts/make_table_s5_persistence.py
python3 manuscript/scripts/make_tables_s3_s4_s6.py
```

## Citation and license

Please cite the preprint (citation to be added) and this repository (`CITATION.cff`). Code is released under the
Apache License 2.0. The measure specifications are CMS's; the datasets keep their own licenses.

**Terminology licenses.** The code uses value sets from the Value Set Authority Center and RxNorm, which you
download with your own free UMLS license, and refers to a few SNOMED CT and LOINC codes. The repository holds only
those few identifiers. The NLM acknowledgment and the SNOMED CT and LOINC notices are in [`NOTICE`](NOTICE).
