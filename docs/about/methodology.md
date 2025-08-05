# Our Analytical Methodology

## Sabermetrics-Inspired Approach

Our analytical framework is built on principles established by pioneers like Bill James and refined by researchers like Jim Albert, adapted specifically for the unique characteristics of professional soccer.

### Core Philosophical Principles

#### 1. **Predictive Power Over Descriptive Appeal**
We prioritize metrics that correlate with future success over those that simply sound impressive.

!!! example "Baseball Lesson Applied"
    Jim Albert's research showed that batting average had a year-over-year correlation of only 0.44, while On-Base Percentage (OBP) correlated at 0.61. We apply the same rigorous testing to soccer metrics, identifying which statistics actually predict future performance.

#### 2. **Context-Independent Analysis**
Following the Defense Independent Pitching Statistics (DIPS) concept, we separate individual performance from team and situational factors wherever possible.

#### 3. **Composite Metrics Over Simple Counting**
Like baseball's move from batting average to OPS, we combine multiple performance dimensions into meaningful composite measures.

#### 4. **Historical Benchmarking**
All performance is evaluated relative to historical context and peer groups, not in absolute terms.

## The NWSL Impact Rating (NIR) System

### Conceptual Foundation

Our flagship metric, the NWSL Impact Rating (NIR), represents a soccer adaptation of composite metrics like OPS (On-base Plus Slugging) in baseball.

**NIR Formula:**
```
NIR = (Attacking Impact × 0.4) + (Defensive Impact × 0.3) + (Progression Impact × 0.2) + Context Adjustment
```

### Component Breakdown

#### Attacking Impact
Combines goals and assists per 90 minutes, weighted by shot efficiency:
- Raw production: Goals + Assists per 90
- Efficiency multiplier: Shot accuracy (minimum 0.5 to avoid small sample bias)
- **Why this matters**: Separates efficient attackers from high-volume, low-efficiency players

#### Defensive Impact  
Measures defensive actions per 90 minutes:
- Tackles + Interceptions + Blocks per 90
- Adjusted for playing time to ensure fair comparison
- **Why this matters**: Captures defensive contribution often invisible in traditional stats

#### Progression Impact
Evaluates ball progression and distribution:
- Progressive passes per 90 × (0.5 + passing accuracy × 0.5)
- **Why this matters**: Identifies players who advance team possession effectively

#### Context Adjustment
*Currently neutral (1.0), future enhancement will include:*
- Opposition strength weighting
- Team quality adjustment  
- Game state considerations
- Home/away effects

### Validation Methodology

We test all metrics using Jim Albert's year-over-year correlation approach:

1. **Calculate metric** for all players in Season N
2. **Recalculate same metric** for same players in Season N+1  
3. **Measure correlation** between the two seasons
4. **Higher correlation = more predictive value**

!!! note "Transparency Commitment"
    We publish correlation coefficients for all our metrics and continuously test new approaches. Metrics that don't demonstrate predictive power are deprecated.

## Data Processing Standards

### Quality Assurance Framework

#### 1. **Source Validation**
- Primary sources: FBRef match pages with detailed statistics
- Secondary validation: Official NWSL records where available
- Manual verification: Edge cases and outliers reviewed individually

#### 2. **Completeness Standards**
- **Target**: 100% coverage for all available matches
- **Current**: 99.38% completion across 42,572 player records
- **Missing data handling**: Explicit documentation of gaps, never filled with estimates

#### 3. **Consistency Checks**
- Cross-season format validation
- Statistical outlier identification and verification
- Historical data integrity maintenance

### Statistical Processing

#### Age Standardization
Player ages parsed from historical formats:
- Legacy format: "YY-DDD" (year-day of year)
- Modern format: Standard decimal years
- Consistency maintained across all 13 seasons

#### Position Mapping
Standardized position categories:
- Historical variations normalized
- Tactical role identification beyond simple positions
- Formation-agnostic classification system

#### Sample Size Requirements
Minimum thresholds for meaningful analysis:
- Players: 3+ matches with >0 minutes for seasonal analysis
- Teams: Full season data required for comparative analysis
- Historical trends: Minimum 2 seasons for longitudinal studies

## Analytical Limitations & Assumptions

### What We Don't Capture
- **Goalkeeper-specific metrics**: Currently limited by data availability
- **Defensive positioning**: Individual marking and spatial coverage
- **Set piece specialization**: Corner/free kick specific contributions
- **Leadership/intangibles**: Qualitative factors beyond statistical measurement

### Known Biases
- **Playing time bias**: Higher minutes = more opportunities for statistical accumulation
- **Team quality effect**: Players on better teams may have inflated metrics
- **Competition level**: Regular season vs. playoff performance may differ

### Methodological Assumptions
- **Linear metric combination**: NIR components combined additively (future versions may explore non-linear relationships)
- **Position neutrality**: Current metrics don't adjust for positional expectations
- **Temporal consistency**: Assumes tactical/competitive environment remains relatively stable

## Continuous Improvement Process

### Research Pipeline
1. **Hypothesis Development**: Based on soccer tactical theory and statistical research
2. **Historical Testing**: Validate against 13 seasons of NWSL data
3. **Correlation Analysis**: Measure predictive power using Albert's methodology
4. **Implementation**: Deploy metrics meeting validation thresholds
5. **Monitoring**: Ongoing performance tracking and refinement

### Community Input
We actively seek feedback from:
- Soccer analysts and researchers
- NWSL coaches and technical staff  
- Statistics and data science community
- Engaged fans with tactical knowledge

---

*Our methodology evolves continuously, but always maintains the core principle: meaningful insights backed by rigorous statistical validation.*