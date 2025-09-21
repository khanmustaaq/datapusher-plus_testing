#!/usr/bin/env python3
"""
Advanced DataPusher Plus Analytics Engine
Provides enterprise-grade insights and predictive analysis
"""

import re
import csv
import sys
import statistics
import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import hashlib

def calculate_data_quality_score(job):
    """Calculate a composite data quality score (0-100)"""
    score = 100
    
    # Penalize based on issues
    if job['valid_csv'] != 'TRUE':
        score -= 30
    if job['sorted'] == 'FALSE':
        score -= 10
    if 'unsafe headers' in job['db_safe_headers'].lower():
        unsafe_count = int(re.search(r'(\d+)', job['db_safe_headers']).group(1)) if re.search(r'(\d+)', job['db_safe_headers']) else 0
        score -= min(unsafe_count * 5, 25)
    if job['normalized'] != 'Successful':
        score -= 20
    if job['analysis'] != 'Successful':
        score -= 25
    
    # Bonus for good characteristics
    if job['encoding'] == 'UTF-8':
        score += 5
    if int(job['records']) > 1000:
        score += 5
    
    return max(0, min(100, score))

def detect_performance_anomalies(jobs):
    """Detect performance anomalies using statistical analysis"""
    if len(jobs) < 3:
        return []
    
    anomalies = []
    total_times = [job['total_time'] for job in jobs if job['status'] == 'SUCCESS']
    
    if len(total_times) >= 3:
        mean_time = statistics.mean(total_times)
        stdev_time = statistics.stdev(total_times)
        threshold = mean_time + (2 * stdev_time)
        
        for job in jobs:
            if job['status'] == 'SUCCESS' and job['total_time'] > threshold:
                anomalies.append({
                    'file': job['file_name'],
                    'job_id': job['job_id'],
                    'actual_time': job['total_time'],
                    'expected_time': mean_time,
                    'deviation_factor': job['total_time'] / mean_time,
                    'type': 'SLOW_PROCESSING'
                })
    
    return anomalies

def analyze_failure_patterns(jobs):
    """Advanced failure pattern analysis"""
    error_jobs = [job for job in jobs if job['status'] == 'ERROR']
    patterns = {
        'by_file_format': Counter(),
        'by_time_of_day': Counter(),
        'by_file_size_proxy': Counter(),  # Using records as proxy
        'sequential_failures': [],
        'recurring_files': Counter()
    }
    
    for job in error_jobs:
        patterns['by_file_format'][job['file_format']] += 1
        
        # Time-based analysis
        try:
            dt = datetime.strptime(job['timestamp'], '%Y-%m-%d %H:%M:%S')
            hour_bucket = f"{dt.hour:02d}:00-{dt.hour:02d}:59"
            patterns['by_time_of_day'][hour_bucket] += 1
        except:
            pass
        
        # File size analysis (using records as proxy)
        records = int(job['records']) if job['records'] else 0
        size_bucket = 'small' if records < 100 else 'medium' if records < 10000 else 'large'
        patterns['by_file_size_proxy'][size_bucket] += 1
        
        patterns['recurring_files'][job['file_name']] += 1
    
    # Detect sequential failures
    error_jobs_sorted = sorted(error_jobs, key=lambda x: x['timestamp'])
    consecutive_count = 0
    for i, job in enumerate(error_jobs_sorted):
        if i > 0:
            prev_time = datetime.strptime(error_jobs_sorted[i-1]['timestamp'], '%Y-%m-%d %H:%M:%S')
            curr_time = datetime.strptime(job['timestamp'], '%Y-%m-%d %H:%M:%S')
            if (curr_time - prev_time).seconds < 300:  # Within 5 minutes
                consecutive_count += 1
            else:
                if consecutive_count > 0:
                    patterns['sequential_failures'].append(consecutive_count + 1)
                consecutive_count = 0
    
    return patterns

def calculate_processing_efficiency_metrics(jobs):
    """Calculate advanced efficiency metrics"""
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    if not successful_jobs:
        return {}
    
    metrics = {}
    
    # Records per second throughput
    total_records = sum(int(job['records']) for job in successful_jobs)
    total_time = sum(job['total_time'] for job in successful_jobs)
    metrics['overall_throughput'] = total_records / total_time if total_time > 0 else 0
    
    # Phase efficiency analysis
    phase_times = {
        'download': [job['download_time'] for job in successful_jobs],
        'analysis': [job['analysis_time'] for job in successful_jobs],
        'copying': [job['copying_time'] for job in successful_jobs],
        'indexing': [job['indexing_time'] for job in successful_jobs],
        'formulae': [job['formulae_time'] for job in successful_jobs],
        'metadata': [job['metadata_time'] for job in successful_jobs]
    }
    
    for phase, times in phase_times.items():
        if times:
            metrics[f'{phase}_avg'] = statistics.mean(times)
            metrics[f'{phase}_efficiency'] = sum(times) / total_time * 100  # % of total time
    
    # Resource utilization scoring
    for job in successful_jobs:
        records = int(job['records'])
        if records > 0:
            job['records_per_second'] = records / job['total_time']
            job['time_per_1k_records'] = job['total_time'] / (records / 1000) if records >= 1000 else job['total_time']
    
    return metrics

def generate_predictive_insights(jobs):
    """Generate predictive insights and recommendations"""
    insights = []
    
    # Failure prediction based on patterns
    error_jobs = [job for job in jobs if job['status'] == 'ERROR']
    if error_jobs:
        error_formats = Counter(job['file_format'] for job in error_jobs)
        total_jobs_by_format = Counter(job['file_format'] for job in jobs)
        
        for fmt, error_count in error_formats.items():
            total_count = total_jobs_by_format[fmt]
            failure_rate = error_count / total_count
            if failure_rate > 0.3:  # 30% failure rate
                insights.append({
                    'type': 'HIGH_RISK_FORMAT',
                    'format': fmt,
                    'failure_rate': failure_rate,
                    'recommendation': f'Review {fmt} file processing pipeline - {failure_rate:.1%} failure rate detected'
                })
    
    # Performance degradation detection
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    if len(successful_jobs) >= 6:
        # Compare first half vs second half performance
        mid_point = len(successful_jobs) // 2
        first_half = successful_jobs[:mid_point]
        second_half = successful_jobs[mid_point:]
        
        avg_first = statistics.mean(job['total_time'] for job in first_half)
        avg_second = statistics.mean(job['total_time'] for job in second_half)
        
        if avg_second > avg_first * 1.3:  # 30% slower
            insights.append({
                'type': 'PERFORMANCE_DEGRADATION',
                'degradation_factor': avg_second / avg_first,
                'recommendation': 'System performance degrading over time - investigate resource constraints'
            })
    
    # Data quality trend analysis
    quality_scores = [calculate_data_quality_score(job) for job in successful_jobs]
    if quality_scores and statistics.mean(quality_scores) < 80:
        insights.append({
            'type': 'DATA_QUALITY_CONCERN',
            'avg_quality_score': statistics.mean(quality_scores),
            'recommendation': 'Multiple data quality issues detected - implement data validation pipeline'
        })
    
    return insights

def generate_business_impact_metrics(jobs):
    """Calculate business-relevant metrics"""
    metrics = {}
    
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    total_jobs = len(jobs)
    
    # Availability metrics
    metrics['system_availability'] = len(successful_jobs) / total_jobs if total_jobs > 0 else 0
    metrics['mttr'] = calculate_mean_time_to_recovery(jobs)  # Simplified
    
    # Data pipeline health
    total_records = sum(int(job['records']) for job in successful_jobs)
    total_processing_time = sum(job['total_time'] for job in successful_jobs)
    
    metrics['data_pipeline_efficiency'] = total_records / total_processing_time if total_processing_time > 0 else 0
    metrics['cost_per_1k_records'] = estimate_processing_cost(successful_jobs)
    
    # Quality impact
    quality_scores = [calculate_data_quality_score(job) for job in successful_jobs]
    metrics['avg_data_quality'] = statistics.mean(quality_scores) if quality_scores else 0
    metrics['quality_sla_compliance'] = sum(1 for score in quality_scores if score >= 85) / len(quality_scores) if quality_scores else 0
    
    return metrics

def calculate_mean_time_to_recovery(jobs):
    """Simplified MTTR calculation"""
    # This would need more sophisticated logic in production
    error_jobs = [job for job in jobs if job['status'] == 'ERROR']
    if not error_jobs:
        return 0
    return statistics.mean(job['total_time'] for job in error_jobs)

def estimate_processing_cost(jobs):
    """Estimate processing cost based on resource usage"""
    # Simplified cost model - in reality would integrate with cloud billing APIs
    total_cpu_seconds = sum(job['total_time'] for job in jobs)
    # Assuming $0.10 per CPU hour as rough estimate
    return (total_cpu_seconds / 3600) * 0.10

def generate_security_insights(jobs):
    """Security and compliance insights"""
    insights = []
    
    # Detect suspicious patterns
    file_hashes = {}
    for job in jobs:
        # Simple hash of filename + file format for duplicate detection
        file_sig = hashlib.md5(f"{job['file_name']}{job['file_format']}".encode()).hexdigest()
        if file_sig in file_hashes:
            file_hashes[file_sig].append(job)
        else:
            file_hashes[file_sig] = [job]
    
    # Flag potential security issues
    for file_sig, job_list in file_hashes.items():
        if len(job_list) > 5:  # Same file processed many times
            insights.append({
                'type': 'SUSPICIOUS_ACTIVITY',
                'pattern': 'REPEATED_FILE_PROCESSING',
                'file': job_list[0]['file_name'],
                'count': len(job_list),
                'recommendation': 'Investigate repeated processing of same file'
            })
    
    # Data compliance scoring
    encoding_compliance = sum(1 for job in jobs if job['encoding'] == 'UTF-8') / len(jobs) if jobs else 0
    header_compliance = sum(1 for job in jobs if 'All headers safe' in job['db_safe_headers']) / len(jobs) if jobs else 0
    
    compliance_score = (encoding_compliance + header_compliance) / 2 * 100
    insights.append({
        'type': 'COMPLIANCE_SCORE',
        'score': compliance_score,
        'encoding_compliance': encoding_compliance * 100,
        'header_safety_compliance': header_compliance * 100
    })
    
    return insights

def enhanced_parse_worker_logs(log_file_path):
    """Enhanced parsing with additional intelligence"""
    # Use the existing parse_worker_logs function
    jobs = parse_worker_logs(log_file_path)
    
    # Add calculated fields
    for job in jobs:
        if job['status'] == 'SUCCESS':
            job['data_quality_score'] = calculate_data_quality_score(job)
            job['processing_efficiency'] = int(job['records']) / job['total_time'] if job['total_time'] > 0 else 0
    
    return jobs

def write_enhanced_analysis(jobs, output_file):
    """Write comprehensive analysis including new metrics"""
    # Standard analysis
    write_worker_analysis(jobs, output_file)
    
    # Generate additional analysis files
    base_path = Path(output_file).parent
    
    # Performance insights
    performance_metrics = calculate_processing_efficiency_metrics(jobs)
    with open(base_path / 'performance_metrics.json', 'w') as f:
        json.dump(performance_metrics, f, indent=2)
    
    # Business metrics
    business_metrics = generate_business_impact_metrics(jobs)
    with open(base_path / 'business_metrics.json', 'w') as f:
        json.dump(business_metrics, f, indent=2)
    
    # Failure analysis
    failure_patterns = analyze_failure_patterns(jobs)
    with open(base_path / 'failure_analysis.json', 'w') as f:
        json.dump(failure_patterns, f, indent=2, default=str)
    
    # Predictive insights
    predictions = generate_predictive_insights(jobs)
    with open(base_path / 'predictive_insights.json', 'w') as f:
        json.dump(predictions, f, indent=2)
    
    # Security insights
    security = generate_security_insights(jobs)
    with open(base_path / 'security_analysis.json', 'w') as f:
        json.dump(security, f, indent=2)
    
    # Anomalies
    anomalies = detect_performance_anomalies(jobs)
    with open(base_path / 'anomalies.json', 'w') as f:
        json.dump(anomalies, f, indent=2)

def generate_executive_summary(jobs):
    """Generate C-level executive summary"""
    total_jobs = len(jobs)
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    
    summary = {
        'executive_summary': {
            'system_health': 'HEALTHY' if len(successful_jobs)/total_jobs >= 0.95 else 'DEGRADED' if len(successful_jobs)/total_jobs >= 0.80 else 'CRITICAL',
            'availability_sla': len(successful_jobs)/total_jobs * 100,
            'total_data_processed': f"{sum(int(job['records']) for job in successful_jobs):,} records",
            'average_processing_time': f"{statistics.mean([job['total_time'] for job in successful_jobs]):.2f}s" if successful_jobs else "N/A",
            'cost_efficiency_score': min(100, max(0, 100 - estimate_processing_cost(successful_jobs) * 1000)),  # Scaled for display
            'data_quality_grade': get_quality_grade(jobs),
            'key_recommendations': generate_top_recommendations(jobs)
        }
    }
    
    return summary

def get_quality_grade(jobs):
    """Convert quality scores to letter grades"""
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    if not successful_jobs:
        return 'F'
    
    quality_scores = [calculate_data_quality_score(job) for job in successful_jobs]
    avg_score = statistics.mean(quality_scores)
    
    if avg_score >= 90: return 'A'
    elif avg_score >= 80: return 'B'
    elif avg_score >= 70: return 'C'
    elif avg_score >= 60: return 'D'
    else: return 'F'

def generate_top_recommendations(jobs):
    """Generate top 3 actionable recommendations"""
    recommendations = []
    
    # Analyze patterns and generate smart recommendations
    error_jobs = [job for job in jobs if job['status'] == 'ERROR']
    if len(error_jobs) / len(jobs) > 0.1:
        recommendations.append("Implement pre-processing validation to reduce 10%+ failure rate")
    
    successful_jobs = [job for job in jobs if job['status'] == 'SUCCESS']
    if successful_jobs:
        avg_time = statistics.mean(job['total_time'] for job in successful_jobs)
        if avg_time > 5:
            recommendations.append("Optimize processing pipeline - average 5+ second processing time")
        
        quality_scores = [calculate_data_quality_score(job) for job in successful_jobs]
        if statistics.mean(quality_scores) < 80:
            recommendations.append("Implement data quality gates - current average below 80%")
    
    return recommendations[:3]

# Update the main function to include new commands
def main():
    if len(sys.argv) < 2:
        print("Usage: python log_analyzer.py <command> [args...]")
        print("Commands:")
        print("  analyze <log_file> <output_csv>")
        print("  insights <worker_csv>")
        print("  file-insight <worker_csv> <filename>")
        print("  executive-summary <worker_csv>")
        print("  anomalies <worker_csv>")
        print("  business-metrics <worker_csv>")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "analyze":
        if len(sys.argv) < 4:
            print("Usage: python log_analyzer.py analyze <log_file> <output_csv>")
            sys.exit(1)
        
        log_file = sys.argv[2]
        output_csv = sys.argv[3]
        
        jobs = enhanced_parse_worker_logs(log_file)
        write_enhanced_analysis(jobs, output_csv)
        print(f"Enhanced analysis complete: {len(jobs)} jobs processed")
        
    elif command == "executive-summary":
        if len(sys.argv) < 3:
            print("Usage: python log_analyzer.py executive-summary <worker_csv>")
            sys.exit(1)
        
        jobs = load_jobs_from_csv(sys.argv[2])
        summary = generate_executive_summary(jobs)
        print(json.dumps(summary, indent=2))
        
    elif command == "anomalies":
        if len(sys.argv) < 3:
            print("Usage: python log_analyzer.py anomalies <worker_csv>")
            sys.exit(1)
            
        jobs = load_jobs_from_csv(sys.argv[2])
        anomalies = detect_performance_anomalies(jobs)
        for anomaly in anomalies:
            print(f"ANOMALY: {anomaly['file']} took {anomaly['actual_time']:.2f}s ({anomaly['deviation_factor']:.1f}x expected)")
    
    elif command == "business-metrics":
        if len(sys.argv) < 3:
            print("Usage: python log_analyzer.py business-metrics <worker_csv>")
            sys.exit(1)
            
        jobs = load_jobs_from_csv(sys.argv[2])
        metrics = generate_business_impact_metrics(jobs)
        for metric, value in metrics.items():
            if isinstance(value, float):
                print(f"{metric}: {value:.3f}")
            else:
                print(f"{metric}: {value}")
    
    # ... existing commands remain the same ...

def load_jobs_from_csv(csv_file):
    """Load jobs from CSV with proper type conversion"""
    jobs = []
    try:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            jobs = list(reader)
            # Convert numeric fields
            for job in jobs:
                for field in ['total_time', 'download_time', 'analysis_time', 'copying_time', 
                              'indexing_time', 'formulae_time', 'metadata_time']:
                    job[field] = float(job[field]) if job[field] else 0.0
                for field in ['records', 'rows_copied', 'columns_indexed']:
                    job[field] = int(job[field]) if job[field] else 0
    except FileNotFoundError:
        print("Worker analysis file not found")
        sys.exit(1)
    return jobs

if __name__ == "__main__":
    main()
