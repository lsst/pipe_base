"""Statistical analysis functions for the runtime tracker.

Provides Cohen's d, Welch's t-test approximation, Mann-Whitney U test,
linear regression, and significance evaluation.
"""

from __future__ import annotations

import math


def cohens_d(
    mean1: float, std1: float, n1: int,
    mean2: float, std2: float, n2: int,
) -> float:
    """Compute Cohen's d effect size using pooled standard deviation.

    Parameters
    ----------
    mean1 : `float`
        Mean of group 1.
    std1 : `float`
        Standard deviation of group 1.
    n1 : `int`
        Sample size of group 1.
    mean2 : `float`
        Mean of group 2.
    std2 : `float`
        Standard deviation of group 2.
    n2 : `int`
        Sample size of group 2.

    Returns
    -------
    d : `float`
        Cohen's d.  Positive means group 1 has higher values; ``0.0``
        when either sample size is below 2.
    """
    if n1 < 2 or n2 < 2:
        return 0.0
    pooled_var = (((n1 - 1) * std1 ** 2 + (n2 - 1) * std2 ** 2) / (n1 + n2 - 2))
    pooled_std = math.sqrt(pooled_var) if pooled_var > 0 else 1e-15
    return (mean1 - mean2) / pooled_std


def classify_effect(size: float) -> str:
    """Classify the magnitude of an effect size.

    Parameters
    ----------
    size : `float`
        Absolute value of Cohen's d.

    Returns
    -------
    category : `str`
        One of ``"trivial"``, ``"small"``, ``"medium"``, ``"large"``.
    """
    abs_size = abs(size)
    if abs_size < 0.2:
        return "trivial"
    elif abs_size < 0.5:
        return "small"
    elif abs_size < 0.8:
        return "medium"
    else:
        return "large"


def welch_t_test_approx(
    mean1: float, std1: float, n1: int,
    mean2: float, std2: float, n2: int,
) -> float:
    """Compute approximate Welch's t-test p-value from summary metrics.

    Forms the Welch t-statistic and Welch-Satterthwaite degrees of
    freedom from the summary statistics; the two-tailed p-value comes
    from `scipy.stats.t` when available, otherwise from a regularized
    incomplete beta evaluation of the Student-t tail.

    Parameters
    ----------
    mean1 : `float`
        Mean of group 1.
    std1 : `float`
        Standard deviation of group 1.
    n1 : `int`
        Sample size of group 1.
    mean2 : `float`
        Mean of group 2.
    std2 : `float`
        Standard deviation of group 2.
    n2 : `int`
        Sample size of group 2.

    Returns
    -------
    p_value : `float`
        Two-tailed p-value.
    """
    if n1 < 2 or n2 < 2:
        return 1.0

    se1 = std1 ** 2 / n1
    se2 = std2 ** 2 / n2
    se_sum = se1 + se2

    if se_sum < 1e-30:
        return 1.0

    t_stat = abs(mean1 - mean2) / math.sqrt(se_sum)

    # Welch-Satterthwaite degrees of freedom
    num = (se1 + se2) ** 2
    denom = (se1 ** 2 / (n1 - 1)) + (se2 ** 2 / (n2 - 1))
    if denom < 1e-30:
        df = float(n1 + n2 - 2)
    else:
        df = num / denom

    # Two-tailed p-value using t-distribution CDF approximation
    return _t_two_tailed_p_value(t_stat, df)


def _t_two_tailed_p_value(t: float, df: float) -> float:
    r"""Two-tailed p-value from a t-statistic with given degrees of freedom.

    The exact two-tailed tail area of a Student-t random variable is

    .. math::

        p = I_x(\nu/2,\; 1/2), \qquad x = \nu/(\nu + t^2)

    where :math:`I_x` is the regularized incomplete beta function.
    ``scipy.stats.t.sf`` is used when available; otherwise a hand-rolled
    continued-fraction evaluation of :math:`I_x(\nu/2, 1/2)` (with the
    correct second shape parameter ``b = 1/2``) is used.

    Parameters
    ----------
    t : `float`
        The (signed) t-statistic; the test is symmetric so ``|t|`` is used.
    df : `float`
        Degrees of freedom (must be > 0).

    Returns
    -------
    p_value : `float`
        Two-tailed p-value in ``[0, 1]``.
    """
    t = abs(float(t))
    df = float(df)
    if df <= 0:
        return 1.0
    if t < 1e-10:
        return 1.0

    # Preferred path: exact survival function from scipy.
    try:
        from scipy.stats import t as t_dist
    except ImportError:
        pass
    else:
        return float(2.0 * t_dist.sf(t, df))

    # Fallback (no scipy): regularized incomplete beta I_x(df/2, 1/2).
    a = df / 2.0
    b = 0.5
    x = df / (df + t * t)
    return _incomplete_beta_cf(x, a, b)


def _normal_sf(z: float) -> float:
    """Survival function (1 - CDF) of the standard normal distribution.

    ``scipy.stats.norm.sf`` is used when available. Otherwise a rational
    polynomial approximation (Abramowitz & Stegun 26.2.17) is used, applied
    via the distribution's symmetry so that negative arguments are handled
    correctly: ``sf(z) = cdf(-z)`` for ``z < 0``.
    """
    z = float(z)

    # Preferred path: exact survival function from scipy.
    try:
        from scipy.stats import norm
    except ImportError:
        pass
    else:
        return float(norm.sf(z))

    # Fallback (no scipy): use symmetry for negative z so we always evaluate
    # the polynomial on its valid domain (z >= 0). sf(z) = cdf(-z).
    if z < 0:
        return _normal_cdf(-z)
    if z > 8:
        return 0.0

    # Constants for Abramowitz & Stegun 26.2.17 approximation.
    b1 = 0.319381530
    b2 = -0.356563782
    b3 = 1.781477937
    b4 = -1.821255978
    b5 = 1.330274429
    p = 0.2316419

    t = 1.0 / (1.0 + p * z)
    poly = t * (b1 + t * (b2 + t * (b3 + t * (b4 + t * b5))))
    phi = 1.0 / math.sqrt(2.0 * math.pi) * math.exp(-0.5 * z * z)
    return phi * poly


def _normal_cdf(z: float) -> float:
    """Cumulative distribution function of the standard normal distribution."""
    if z < -8:
        return 0.0
    if z > 8:
        return 1.0
    return 1.0 - _normal_sf(z)


def _incomplete_beta_cf(x: float, a: float, b: float, max_iter: int = 200) -> float:
    """Compute the regularized incomplete beta function I_x(a, b) using
    Lentz's continued fraction algorithm.
    """
    if x < 0 or x > 1:
        return 0.0
    if x == 0:
        return 0.0
    if x == 1:
        return 1.0

    # Use symmetry: I_x(a,b) = 1 - I_{1-x}(b,a) when x > (a+1)/(a+b+2)
    threshold = (a + 1) / (a + b + 2)
    if x > threshold:
        return 1.0 - _incomplete_beta_cf(1.0 - x, b, a, max_iter)

    # Log of the prefactor: x^a * (1-x)^b / (a * B(a,b))
    lbeta = _ln_gamma(a) + _ln_gamma(b) - _ln_gamma(a + b)
    log_front = a * math.log(x) + b * math.log(1.0 - x) - lbeta - math.log(a)
    front = math.exp(log_front)

    # Continued fraction using Lentz's method
    f = 1.0
    c = 1.0
    d = 1.0 - (a + b) * x / (a + 1.0)
    if abs(d) < 1e-30:
        d = 1e-30
    d = 1.0 / d
    f = d

    for m in range(1, max_iter + 1):
        # Even step
        numerator = m * (b - m) * x / ((a + 2 * m - 1) * (a + 2 * m))
        d = 1.0 + numerator * d
        if abs(d) < 1e-40:
            d = 1e-40
        c = 1.0 + numerator / c
        if abs(c) < 1e-40:
            c = 1e-40
        d = 1.0 / d
        delta = c * d
        f *= delta

        if abs(delta - 1.0) < 1e-12:
            break

        # Odd step
        numerator = -(a + m) * (a + b + m) * x / ((a + 2 * m) * (a + 2 * m + 1))
        d = 1.0 + numerator * d
        if abs(d) < 1e-40:
            d = 1e-40
        c = 1.0 + numerator / c
        if abs(c) < 1e-40:
            c = 1e-40
        d = 1.0 / d
        delta = c * d
        f *= delta

        if abs(delta - 1.0) < 1e-12:
            break

    result = front * f
    # Clamp to [0, 1]
    return max(0.0, min(1.0, result))


def _ln_gamma(x: float) -> float:
    """Log of the gamma function using the Lanczos approximation."""
    if x <= 0.0:
        return 0.0
    # Lanczos approximation
    g = 7
    coef = [
        0.99999999999980993,
        676.5203681218851,
        -1259.1392167224028,
        771.32342877765313,
        -176.61502916214059,
        12.507343278686905,
        -0.13857109526572012,
        9.9843695780195716e-6,
        1.5056327351493116e-7,
    ]
    if x < 0.5:
        return math.log(math.pi / math.sin(math.pi * x)) - _ln_gamma(1.0 - x)
    x -= 1.0
    y = coef[0]
    for i in range(1, g + 2):
        y += coef[i] / (x + i)
    t = x + g + 0.5
    return 0.5 * math.log(2.0 * math.pi) + (x + 0.5) * math.log(t) - t + math.log(y)


def mann_whitney_u_test(group1: list, group2: list) -> tuple:
    """Compute Mann-Whitney U test statistic and p-value.

    Wraps `scipy.stats.mannwhitneyu` (two-sided alternative).

    Parameters
    ----------
    group1 : `list` or `numpy.ndarray`
        Observations from group 1.
    group2 : `list` or `numpy.ndarray`
        Observations from group 2.

    Returns
    -------
    u_stat : `float`
        Mann-Whitney U statistic.
    p_value : `float`
        Two-tailed p-value.

    Raises
    ------
    ImportError
        Raised if ``scipy`` is not installed.
    """
    try:
        from scipy.stats import mannwhitneyu
    except ImportError:
        raise ImportError(
            "scipy is required for Mann-Whitney U test. "
            "Install it with: pip install scipy"
        )

    result = mannwhitneyu(group1, group2, alternative="two-sided")
    return float(result.statistic), float(result.pvalue)


def linear_regression(
    x: list, y: list
) -> tuple:
    """Compute OLS linear regression.

    Parameters
    ----------
    x : `list` or `numpy.ndarray`
        Independent variable values.
    y : `list` or `numpy.ndarray`
        Dependent variable values.

    Returns
    -------
    slope : `float`
        Slope of the regression line.
    intercept : `float`
        Intercept of the regression line.
    r_squared : `float`
        Coefficient of determination.
    p_value : `float`
        Two-tailed p-value for the slope (from scipy if available).
    """
    n = len(x)
    if n < 2:
        return 0.0, 0.0, 0.0, 1.0

    x_arr = list(x)
    y_arr = list(y)

    x_mean = sum(x_arr) / n
    y_mean = sum(y_arr) / n

    ss_xy = sum((x_arr[i] - x_mean) * (y_arr[i] - y_mean) for i in range(n))
    ss_xx = sum((x_arr[i] - x_mean) ** 2 for i in range(n))
    ss_yy = sum((y_arr[i] - y_mean) ** 2 for i in range(n))

    if ss_xx < 1e-30:
        return 0.0, y_mean, 0.0, 1.0

    slope = ss_xy / ss_xx
    intercept = y_mean - slope * x_mean

    # R-squared
    ss_res = sum((y_arr[i] - (slope * x_arr[i] + intercept)) ** 2 for i in range(n))
    r_squared = 1.0 - ss_res / ss_yy if ss_yy > 1e-30 else 0.0

    # p-value for slope
    if n > 2:
        if ss_res < 1e-30 and abs(slope) > 1e-15:
            # Perfect fit with non-zero slope: p is essentially 0
            p_value = 0.0
        elif ss_res > 0:
            se_res = math.sqrt(ss_res / (n - 2))
            se_slope = se_res / math.sqrt(ss_xx) if ss_xx > 1e-30 else 1e-15
            t_stat = abs(slope) / se_slope
            p_value = _t_two_tailed_p_value(t_stat, n - 2)
        else:
            # ss_res = 0 but slope is ~0 (all y values equal)
            p_value = 1.0
    else:
        p_value = 1.0

    return slope, intercept, r_squared, p_value


def evaluate_significance(c_d: float, p_value: float) -> str:
    """Classify the significance of a change.

    A change is "significant" only if p < 0.05 AND |effect| > 0.3.
    "stable" if effect size is trivial (|d| < 0.2).
    "uncertain" otherwise.

    Parameters
    ----------
    c_d : `float`
        Cohen's d effect size.
    p_value : `float`
        Two-tailed p-value.

    Returns
    -------
    status : `str`
        One of ``"stable"``, ``"uncertain"``, ``"significant"``.
    """
    if abs(c_d) < 0.2:
        return "stable"
    if p_value < 0.05 and abs(c_d) > 0.3:
        return "significant"
    return "uncertain"
