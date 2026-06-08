from prefect import flow, task, get_run_logger
import subprocess
import re
import os

@task
def create_minimap2_index(reference_fasta: str, index_output: str) -> str:
    logger = get_run_logger()
    if os.path.exists(index_output):
        logger.info(f"Index {index_output} already exists, skipping")
        return index_output
    try:
        logger.info(f"Creating minimap2 index for {reference_fasta}")
        subprocess.run(["minimap2", "-d", index_output, reference_fasta],
                       capture_output=True, text=True, check=True)
        logger.info(f"Index created: {index_output}")
        return index_output
    except subprocess.CalledProcessError as e:
        logger.error(f"Indexing failed: {e.stderr}")
        raise

@task
def align_reads(index_file: str, reads_file: str, output_sam: str) -> str:
    logger = get_run_logger()
    try:
        logger.info(f"Aligning {reads_file} to {index_file}")
        with open(output_sam, "w") as f:
            subprocess.run(["minimap2", "-a", index_file, reads_file],
                           stdout=f, stderr=subprocess.PIPE, text=True, check=True)
        logger.info(f"Alignment saved to {output_sam}")
        return output_sam
    except subprocess.CalledProcessError as e:
        logger.error(f"Alignment failed: {e.stderr}")
        raise

@task
def sort_sam_to_bam(sam_file: str, sorted_bam: str) -> str:
    logger = get_run_logger()
    try:
        logger.info(f"Converting and sorting {sam_file} -> {sorted_bam}")
        subprocess.run(["samtools", "sort", sam_file, "-o", sorted_bam],
                       capture_output=True, text=True, check=True)
        logger.info(f"Sorted BAM: {sorted_bam}")
        subprocess.run(["samtools", "index", sorted_bam], check=True)
        return sorted_bam
    except subprocess.CalledProcessError as e:
        logger.error(f"samtools sort/index failed: {e.stderr}")
        raise

@task
def check_alignment_quality(bam_file: str, threshold_good: float = 80.0) -> str:
    """Run samtools flagstat, return mapping percentage and status (OK/NOT OK)"""
    logger = get_run_logger()
    try:
        result = subprocess.run(["samtools", "flagstat", bam_file],
                                capture_output=True, text=True, check=True)
        match = re.search(r'([0-9]+\.[0-9]+)%', result.stdout)
        if not match:
            raise ValueError("Could not parse percentage from flagstat")
        percent = float(match.group(1))
        if percent > threshold_good:
            status = "OK"
        else:
            status = "NOT OK"
        logger.info(f"Mapping percentage: {percent}% -> {status}")
        return f"{percent}:{status}"
    except subprocess.CalledProcessError as e:
        logger.error(f"samtools flagstat failed: {e.stderr}")
        raise

@task
def run_freebayes(reference_fasta: str, bam_file: str, output_vcf: str) -> str:
    logger = get_run_logger()
    try:
        logger.info(f"Running freebayes on {bam_file}")
        with open(output_vcf, "w") as f:
            subprocess.run(["freebayes", "-f", reference_fasta, bam_file],
                           stdout=f, stderr=subprocess.PIPE, text=True, check=True)
        logger.info(f"VCF saved to {output_vcf}")
        return output_vcf
    except subprocess.CalledProcessError as e:
        logger.error(f"Freebayes failed: {e.stderr}")
        raise

@flow(name="Ecoli_Variant_Calling_Pipeline")
def full_pipeline(
    reference_fasta: str = "NC_000913.3.fasta",
    reads_file: str = "SRR33637628.fastq",
    index_mmi: str = "NC_000913.3.fasta.mmi",
    output_sam: str = "output.sam",
    sorted_bam: str = "out.sorted.bam",
    output_vcf: str = "variants.vcf",
    quality_threshold: float = 90.0
) -> dict:
    logger = get_run_logger()
    logger.info("=== Starting full variant calling pipeline ===")

    mmi = create_minimap2_index(reference_fasta, index_mmi)
    sam = align_reads(mmi, reads_file, output_sam)
    bam = sort_sam_to_bam(sam, sorted_bam)
    qual = check_alignment_quality(bam, quality_threshold)
    percent, status = qual.split(":")
    if status == "OK":
        logger.info(f"Quality {status}, proceeding to variant calling")
        vcf = run_freebayes(reference_fasta, bam, output_vcf)
        result = {"quality": qual, "vcf": vcf, "success": True}
    else:
        logger.warning(f"Quality {status} is too low, skipping variant calling")
        result = {"quality": qual, "vcf": None, "success": False}
    logger.info("=== Pipeline finished ===")
    return result

if __name__ == "__main__":
    res = full_pipeline()
    print(f"Result: {res['quality']}, VCF: {res['vcf']}")