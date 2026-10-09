"""Tests for statistics module."""

from __future__ import annotations

import math

import pytest

from lsst.pipe.base._runtime_analyzer.tracker.stats import (
    _incomplete_beta_cf,
    _normal_sf,
    _t_two_tailed_p_value,
    classify_effect,
    cohens_d,
    evaluate_significance,
    linear_regression,
    mann_whitney_u_test,
    welch_t_test_approx,
)

try:
    import scipy.stats as _st
    _HAVE_SCIPY = True
except ImportError:  # pragma: no cover
    _HAVE_SCIPY = False


class TestCohensD:
    """Tests for ``cohens_d``."""

    @pytest.mark.parametrize(("args", "expected", "tol"), [
        ((45.0, 8.0, 100, 45.0, 8.0, 100), 0.0, None),
        ((45.0, 8.0, 1, 52.0, 9.0, 100), 0.0, None),
        # Cohen's d = (mean1 - mean2) / pooled_std
        # pooled_var = ((99*64 + 99*81) / 198) = (6336 + 8019) / 198 = 72.4
        # pooled_std = sqrt(72.4) = 8.51
        ((45.0, 8.0, 100, 52.0, 9.0, 100), -0.822, 0.01),
    ], ids=["equal-distributions", "small-sample", "known-value"])
    def test_cohens_d(self, args, expected, tol):
        """Effect size: identical distributions give 0, degenerate samples
        give 0, and the pooled-std formula matches the known hand value
        (its sign flip covers the positive-d case).
        """
        d = cohens_d(*args)
        if tol is None:
            assert d == expected
        else:
            assert abs(d - expected) < tol


class TestClassifyEffect:
    """Tests for ``classify_effect``."""

    @pytest.mark.parametrize(("d", "expected"), [
        (0.0, "trivial"),
        (0.15, "trivial"),   # just below the 0.2 boundary
        (0.2, "small"),
        (0.5, "medium"),
        (0.8, "large"),
    ], ids=["trivial-0.0", "trivial-0.15", "small-0.2", "medium-0.5",
            "large-0.8"])
    def test_classify_effect_boundaries(self, d, expected):
        """Cohen's d maps onto the trivial/small/medium/large bands at
        their documented (inclusive-lower) boundaries; |d| is used, so
        sign is irrelevant.
        """
        assert classify_effect(d) == expected


class TestWelchTTestApprox:
    """Tests for ``welch_t_test_approx``."""

    @pytest.mark.parametrize(("args", "expected", "mode"), [
        ((45.0, 8.0, 100, 45.0, 8.0, 100), 1.0, "exact"),
        ((45.0, 8.0, 100, 52.0, 8.0, 100), 0.05, "below"),
        ((45.0, 8.0, 1, 52.0, 8.0, 1), 1.0, "exact"),
    ], ids=["equal-means", "different-means-small-p", "small-sample"])
    def test_welch_t_test_approx(self, args, expected, mode):
        """Equal means give p=1, a 7-second shift is significant (p<0.05),
        and degenerate samples return 1.
        """
        p = welch_t_test_approx(*args)
        if mode == "below":
            assert p < expected
        else:
            assert p == expected

    @pytest.mark.skipif(not _HAVE_SCIPY, reason="scipy required for reference")
    def test_clear_regression_is_highly_significant(self):
        # TRK-1 repro: the old wrong formula returned ~0.126 here.
        p = welch_t_test_approx(1.0, 0.4, 10, 2.0, 0.4, 10)
        assert p < 1e-4
        ref = float(
            2.0 * _st.t.sf(abs(1.0 - 2.0) / math.sqrt(0.4**2 / 10 + 0.4**2 / 10),
                           18.0)
        )
        assert p == pytest.approx(ref, abs=1e-6)


class TestLinearRegression:
    """Tests for ``linear_regression``."""

    @pytest.mark.parametrize(("x", "y", "slope", "intercept", "r_sq",
                              "tol", "p_expected"), [
        ([1, 2, 3, 4, 5], [2, 4, 6, 8, 10], 2.0, 0.0, 1.0, 1e-10, 0.0),
        ([1, 2, 3, 4, 5], [5.0, 5.0, 5.0, 5.0, 5.0], 0.0, 5.0, 0.0, 1e-10,
         1.0),
        # Imperfect fit (residual sum of squares > 0): the slope p-value
        # takes the t-statistic branch rather than the perfect-fit 0.0.
        ([1, 2, 3, 4, 5], [2.1, 3.9, 6.2, 7.8, 10.3], 2.03, -0.03, 0.99605,
         1e-4, None),
    ], ids=["perfect-linear", "flat-line", "imperfect-fit"])
    def test_linear_regression_fit(self, x, y, slope, intercept, r_sq, tol,
                                   p_expected):
        """Fits report slope, intercept and r^2 (to ``tol``) and the
        documented slope p-value (``None``: highly significant, i.e.
        0 <= p < 1e-3).
        """
        got_slope, got_intercept, got_r_sq, p = linear_regression(x, y)
        assert abs(got_slope - slope) < tol
        assert abs(got_intercept - intercept) < tol
        assert abs(got_r_sq - r_sq) < tol
        if p_expected is None:
            assert 0.0 <= p < 1e-3
        else:
            assert p == p_expected

    def test_single_point(self):
        slope, intercept, r_sq, p = linear_regression([1], [2])
        assert slope == 0.0
        assert p == 1.0


class TestMannWhitney:
    """Tests for ``mann_whitney_u_test``."""

    def test_import_error_without_scipy(self):
        # This test may skip if scipy is available
        try:
            result = mann_whitney_u_test([1, 2, 3], [4, 5, 6])
            # If scipy is available, check result
            assert isinstance(result, tuple)
            assert len(result) == 2
        except ImportError:
            pass


class TestEvaluateSignificance:
    """Tests for ``evaluate_significance``."""

    @pytest.mark.parametrize(("d", "p", "expected"), [
        (0.1, 0.01, "stable"),
        (0.5, 0.01, "significant"),
        # Distinct uncertain conditions: mid-d/tiny-p vs big-d/big-p.
        (0.25, 0.01, "uncertain"),
        (0.5, 0.1, "uncertain"),
    ], ids=["stable-small-d", "significant-large-d",
            "uncertain-small-d", "uncertain-large-p"])
    def test_evaluate_significance(self, d, p, expected):
        """(effect size, p-value) pairs classify as stable/significant/
        uncertain; |d| is used, so negative-d mirrors are redundant.
        """
        assert evaluate_significance(d, p) == expected


class TestTTwoTailedPValue:
    """TRK-1: two-tailed Student-t p-value uses I_x(df/2, 1/2)."""

    @pytest.mark.parametrize(("t_val", "df", "expected", "mode"), [
        # The exact bug produced ~0.126 here; truth is ~2.6e-5.
        (5.59, 18, None, "tiny"),
        (0.0, 10, 1.0, "one"),
        (3.0, 0, 1.0, "one"),
        (1.5, 12, None, "unit-interval"),
    ], ids=["large-t-is-tiny", "zero-t-is-one", "df-zero-is-one",
            "in-unit-interval"])
    def test_boundary_values(self, t_val, df, expected, mode):
        """Boundary behaviour: huge |t| gives a tiny p, t=0 and
        non-positive df give exactly 1, and results stay in [0, 1].
        """
        p = _t_two_tailed_p_value(t_val, df)
        if mode == "tiny":
            assert p < 1e-4
        elif mode == "one":
            assert p == expected
        else:
            assert 0.0 <= p <= 1.0

    def test_symmetry_sign_ignored(self):
        assert _t_two_tailed_p_value(-5.59, 18) == pytest.approx(
            _t_two_tailed_p_value(5.59, 18), abs=1e-12
        )

    @pytest.mark.parametrize(("t_lo", "t_hi"), [
        (1.0, 2.0),
    ], ids=["1-vs-2"])
    def test_monotonic_in_t(self, t_lo, t_hi):
        assert _t_two_tailed_p_value(t_lo, 20) > _t_two_tailed_p_value(
            t_hi, 20)

    @pytest.mark.parametrize(("t_val", "df"), [
        # t<1, t~2, t>3 (TRK-1 value), small df, large df (P3 grid trim).
        (0.1, 5.0),
        (2.0, 50.0),
        (5.59, 18.0),
        (1.0, 1.5),
        (10.0, 200.0),
    ], ids=["t0.1-df5", "t2-df50", "t5.59-df18", "t1-df1.5", "t10-df200"])
    def test_handrolled_fallback_matches_scipy(self, t_val, df):
        # Exercise the CF fallback path (b=1/2) directly, even when scipy is
        # present, to confirm the continued-fraction implementation itself is
        # correct for non-symmetric parameters.
        if not _HAVE_SCIPY:
            pytest.skip("scipy required for reference values")
        x = df / (df + t_val * t_val)
        hand = _incomplete_beta_cf(x, df / 2.0, 0.5)
        ref = float(2.0 * _st.t.sf(t_val, df))
        assert hand == pytest.approx(ref, abs=1e-6)


class TestNormalSf:
    """TRK-2: standard normal survival function branch/constant."""

    @pytest.mark.parametrize(("z", "expected", "tol"), [
        (-1, 0.8413, 1e-3),
        (0, 0.5, 1e-6),
        (2, 0.0228, 1e-3),
        (100.0, 0.0, "exact"),
        (-100.0, 1.0, 1e-6),
    ], ids=["negative-arg", "zero-arg", "positive-arg",
            "large-positive-is-zero", "large-negative-is-one"])
    def test_normal_sf_values(self, z, expected, tol):
        """Known SF values, including the extreme tails (0 at +inf,
        1 at -inf).
        """
        got = _normal_sf(z)
        if tol == "exact":
            assert got == expected
        else:
            assert got == pytest.approx(expected, abs=tol)
