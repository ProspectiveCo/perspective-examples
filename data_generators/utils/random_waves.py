"""
random_waves.py

This module provides functions to generate various types of waveforms, including full waves and half waves with fixed or varying frequencies and amplitudes. It also includes utility functions for seeding random number generators, dividing points into equal or random parts, and applying modifications to generated waveforms such as phase shifts, smoothing, and noise addition.

Functions:
    seed(seed: int = 42) -> None:
        Seed the random number generators for reproducibility.

    equals_parts(num_points: int = 1000, periods: int = 2) -> list[int]:

    random_parts(num_points: int = 1000, periods: int = 2) -> list[int]:

    full_wave(num_points: int = 1000, periods: int = 1, amplitude: float = 1.0) -> pd.Series:
        Generate a full sine wave with specified number of points, periods, and amplitude.

    half_wave(num_points: int = 500, amplitude: float = 1.0) -> pd.Series:
        Generate a half sine wave with specified number of points and amplitude.

    half_waves_fixed_freq_varying_amp(num_points: int = 1000, periods: int = 3, amplitude: tuple[float, float] = (1.0, 10.0), **kwargs) -> pd.Series:

    half_waves_varying_freq_varying_amp(num_points: int = 1000, periods: int = 3, amplitude: tuple[float, float] = (1.0, 10.0), **kwargs) -> pd.Series:

    full_waves_fixed_freq_varying_amp(num_points: int = 1000, periods: int = 3, amplitude: tuple[float, float] = (1.0, 10.0), **kwargs) -> pd.Series:
        Generate a series of full waves with fixed frequency and varying amplitude.

    full_waves_varying_freq_varying_amp(num_points: int = 1000, periods: int = 3, amplitude: tuple[float, float] = (1.0, 10.0), **kwargs) -> pd.Series:

    apply_phase(wave: pd.Series, phase: float = 0.0) -> pd.Series:

    apply_smooth(wave: pd.Series, window: int = 10) -> pd.Series:
        Smooth a given pandas Series by applying a rolling mean with a specified window size.

    apply_noise(wave: pd.Series, noise: float = 0.1) -> pd.Series:
        Add Gaussian noise to a given wave (time series data).

    apply_wave_modifications(wave: pd.Series, phase: float = None, smooth: int = None, noise: float = None) -> pd.Series:
        Apply modifications to a wave series, including phase shift, smoothing, and noise addition.

"""

import numpy as np
import pandas as pd
import random


__all__ = (
    "seed",
    "full_wave",
    "half_wave",
    "half_waves_fixed_freq_varying_amp",
    "half_waves_varying_freq_varying_amp",
    "full_waves_fixed_freq_varying_amp",
    "full_waves_varying_freq_varying_amp",
)


def seed(seed: int = 42) -> None:
    np.random.seed(seed)
    random.seed(seed)


def equals_parts(num_points: int = 1000, periods: int = 2) -> list[int]:
    """
    Divide a given number of points into approximately equal parts.

    This function takes a total number of points and divides them into a specified number of periods,
    ensuring that the sum of the parts equals the total number of points. The last part will contain
    any remainder to ensure the total number of points is preserved.

    Args:
        num_points (int): The total number of points to be divided. Default is 1000.
        periods (int): The number of periods to divide the points into. Default is 2.

    Returns:
        list[int]: A list containing the number of points in each period.

    Example:
        >>> equals_parts(1000, 3)
        [333, 333, 334]

    """
    parts = [num_points // periods] * periods  # Initialize parts with equal division
    parts[-1] += num_points % periods  # Add the remainder to the last part
    parts = [num_points // periods] * periods
    parts[-1] += num_points % periods
    return parts


def random_parts(num_points: int = 1000, periods: int = 2) -> list[int]:
    """
    Generate a list of random parts that sum up to a specified number of points.

    Args:
        num_points (int): The total number of points to be divided into random parts. Default is 1000.
        periods (int): The number of random parts to generate. Default is 2.

    Returns:
        list[int]: A list of integers where each integer represents a part, and the sum of all parts equals num_points.

    Notes:
        - The function uses a random number generator to create the parts.
        - The sum of the generated parts is adjusted to ensure it exactly matches num_points.
    """
    # Generate random parts that sum up to num_points
    parts = np.random.rand(periods)
    parts /= parts.sum()
    parts *= num_points
    parts = np.round(parts).astype(int)
    # Adjust the last element to ensure the sum is exactly num_points
    parts[-1] += num_points - parts.sum()
    return parts.tolist()


def full_wave(
    num_points: int = 1000,                         # Number of data points
    periods: int = 1,                               # Number of periods in the wave
    amplitude: float = 1.0                          # Amplitude of the wave (default is 1.0)
    ) -> pd.Series:
    period = 2 * np.pi * periods
    x = np.linspace(0, period, num_points)
    y = amplitude * np.sin(x)
    return pd.Series(y)


def half_wave(
    num_points: int = 500,                              # Number of data points
    amplitude: float = 1.0,                             # Amplitude of the wave (default is 1.0)
    ) -> pd.Series:
    x = np.linspace(0, np.pi, num_points)
    y = amplitude * np.sin(x)
    return pd.Series(y)


def half_waves_fixed_freq_varying_amp(
    num_points: int = 1000,                             # Number of data points
    periods: int = 3,                                   # Number of parts in the wave
    amplitude: tuple[float, float] = (1.0, 10.0),       # Amplitude of the wave (default is 1.0)
    **kwargs
    ) -> pd.Series:
    """
    Generate a series of half waves with fixed frequency and varying amplitude.

    Parameters:
    num_points (int): Number of data points to generate. Default is 1000.
    periods (int): Number of parts in the wave. Default is 3.
    amplitude (tuple[float, float]): Tuple representing the range of amplitudes for the wave. Default is (1.0, 10.0).
    **kwargs: Additional keyword arguments for wave modifications.

    Returns:
    pd.Series: A pandas Series containing the generated wave data.
    """
    parts_length = equals_parts(num_points, periods)
    parts = []
    for i in range(periods):
        sign = (-1) ** i
        y = sign * half_wave(parts_length[i], amplitude=random.uniform(amplitude[0], amplitude[1]))
        parts.append(y)
    y = pd.concat(parts)
    # apply wave modifications
    if kwargs:
        y = apply_wave_modifications(y, **kwargs)
    return y


def half_waves_varying_freq_varying_amp(
    num_points: int = 1000,                             # Number of data points
    periods: int = 3,                                   # Number of parts in the wave
    amplitude: tuple[float, float] = (1.0, 10.0),       # Amplitude of the wave (default is 1.0)
    **kwargs
    ) -> pd.Series:
    """
    Generate a series of half waves with varying frequency and amplitude.

    Parameters:
    num_points (int): Number of data points to generate. Default is 1000.
    periods (int): Number of parts in the wave. Default is 3.
    amplitude (tuple[float, float]): Amplitude range of the wave. Default is (1.0, 10.0).
    **kwargs: Additional keyword arguments for wave modifications.

    Returns:
    pd.Series: A pandas Series containing the generated wave data.
    """
    parts_length = random_parts(num_points, periods)
    parts = []
    for i in range(periods):
        sign = (-1) ** i
        y = sign * half_wave(parts_length[i], amplitude=random.uniform(amplitude[0], amplitude[1]))
        parts.append(y)
    y = pd.concat(parts)
    # apply wave modifications
    if kwargs:
        y = apply_wave_modifications(y, **kwargs)
    return y


def full_waves_fixed_freq_varying_amp(
    num_points: int = 1000,                             # Number of data points
    periods: int = 3,                                   # Number of parts in the wave
    amplitude: tuple[float, float] = (1.0, 10.0),       # Amplitude of the wave (default is 1.0)
    **kwargs
    ) -> pd.Series:
    """
    Generate a series of full waves with fixed frequency and amplitude.

    Parameters:
    num_points (int): Number of data points to generate. Default is 1000.
    periods (int): Number of parts in the wave. Default is 3.
    amplitude (tuple[float, float]): Amplitude range of the wave. Default is (1.0, 10.0).
    **kwargs: Additional keyword arguments for wave modifications.

    Returns:
    pd.Series: A pandas Series containing the generated wave data.
    """
    parts_length = equals_parts(num_points, periods)
    parts = []
    for i in range(periods):
        y = full_wave(parts_length[i], periods=1, amplitude=random.uniform(amplitude[0], amplitude[1]))
        parts.append(y)
    y = pd.concat(parts)
    # apply wave modifications
    if kwargs:
        y = apply_wave_modifications(y, **kwargs)
    return y


def full_waves_varying_freq_varying_amp(
    num_points: int = 1000,                             # Number of data points
    periods: int = 3,                                   # Number of parts in the wave
    amplitude: tuple[float, float] = (1.0, 10.0),       # Amplitude of the wave (default is 1.0)
    **kwargs
    ) -> pd.Series:
    """
    Generate a series of full waves with varying frequency and amplitude.

    Parameters:
    num_points (int): Number of data points to generate. Default is 1000.
    periods (int): Number of parts in the wave. Default is 3.
    amplitude (tuple[float, float]): Amplitude range of the wave. Default is (1.0, 10.0).
    **kwargs: Additional keyword arguments for wave modifications.

    Returns:
    pd.Series: A pandas Series containing the generated wave data.
    """
    parts_length = random_parts(num_points, periods)
    parts = []
    for i in range(periods):
        y = full_wave(parts_length[i], periods=1, amplitude=random.uniform(amplitude[0], amplitude[1]))
        parts.append(y)
    y = pd.concat(parts)
    # apply wave modifications
    if kwargs:
        y = apply_wave_modifications(y, **kwargs)
    return y


def apply_phase(wave: pd.Series, phase: float = 0.0) -> pd.Series:
    """
    Apply a phase shift to a wave represented by a pandas Series.

    Parameters:
    wave (pd.Series): A pandas Series representing the wave data.
    phase (float, optional): The phase shift to apply to the wave. Default is 0.0.

    Returns:
    pd.Series: A new pandas Series with the phase-shifted wave.
    """
    x = np.linspace(0, 2 * np.pi, len(wave))
    y = wave * np.sin(x + phase)
    return pd.Series(y)


def apply_smooth(wave: pd.Series, window: int = 10) -> pd.Series:
    """
    Smooths a given pandas Series by applying a rolling mean with a specified window size.

    Parameters:
    wave (pd.Series): The input pandas Series to be smoothed.
    window_size (int, optional): The size of the moving window. Default is 10.

    Returns:
    pd.Series: A new pandas Series with the smoothed values.
    """
    return wave.rolling(window=window).mean().fillna(0)


def apply_noise(wave: pd.Series, noise: float = 0.1) -> pd.Series:
    """
    Adds Gaussian noise to a given wave (time series data).

    Parameters:
    wave (pd.Series): The input time series data to which noise will be added.
    noise_level (float, optional): The standard deviation of the Gaussian noise. Default is 0.1.

    Returns:
    pd.Series: The time series data with added Gaussian noise.
    """
    noise = np.random.normal(0, noise, len(wave))
    return wave + noise


def apply_wave_modifications(
        wave: pd.Series, 
        phase: float = None, 
        smooth: int = None, 
        noise: float = None,
    ) -> pd.Series:
    """
    Apply modifications to a wave series.

    Parameters:
    wave (pd.Series): The input wave series to be modified.
    phase (float, optional): The phase shift to apply to the wave. Default is None.
    smooth (int, optional): The smoothing factor to apply to the wave. Default is None.
    noise (float, optional): The noise level to add to the wave. Default is None.

    Returns:
    pd.Series: The modified wave series.
    """
    if phase:
        wave = apply_phase(wave, phase)
    if smooth:
        wave = apply_smooth(wave, smooth)
    if noise:
        wave = apply_noise(wave, noise)
    return wave
