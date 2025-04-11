/**
 * smooth-scroll.js - Adds smooth scrolling behavior to navigation links
 * 
 * This script enhances the dashboard's navigation by adding smooth scrolling
 * animations when users click on section links in the header.
 */

// Wait for the document to fully load
document.addEventListener('DOMContentLoaded', function() {
    // Add click event listeners to all internal navigation links
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function(e) {
            e.preventDefault();
            
            // Get the target element
            const targetId = this.getAttribute('href');
            const targetElement = document.querySelector(targetId);
            
            if (targetElement) {
                // Calculate position for smooth scrolling
                const headerHeight = document.querySelector('.dashboard-header').offsetHeight;
                const targetPosition = targetElement.getBoundingClientRect().top + window.pageYOffset;
                const offsetPosition = targetPosition - headerHeight - 20; // Add some extra padding
                
                // Perform smooth scrolling
                window.scrollTo({
                    top: offsetPosition,
                    behavior: 'smooth'
                });
                
                // Update URL without page reload (for modern browsers)
                if (history.pushState) {
                    history.pushState(null, null, targetId);
                } else {
                    location.hash = targetId;
                }
            }
        });
    });
    
    // Handle initial page load with hash in URL
    if (window.location.hash) {
        setTimeout(function() {
            const targetElement = document.querySelector(window.location.hash);
            if (targetElement) {
                const headerHeight = document.querySelector('.dashboard-header').offsetHeight;
                const targetPosition = targetElement.getBoundingClientRect().top + window.pageYOffset;
                const offsetPosition = targetPosition - headerHeight - 20;
                
                window.scrollTo({
                    top: offsetPosition,
                    behavior: 'smooth'
                });
            }
        }, 300); // Small delay to ensure all elements are properly loaded
    }
});