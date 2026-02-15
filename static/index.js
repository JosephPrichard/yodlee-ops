function toggleElementHidden(id) {
    let elements = document.getElementsByClassName(id);
    for (let i = 0; i < elements.length; i++) {
       elements[i].classList.toggle('hidden');
    }
}